CREATE MACRO unsigned_integer(value) AS (
    CASE WHEN regexp_full_match(value, '[0-9]+') THEN
        try_cast(coalesce(nullif(ltrim(value, '0'), ''), '0') AS BIGINT)
    END
);

CREATE MACRO event_time(value) AS (
    CASE
        WHEN regexp_full_match(value, '[0-9]{4}-[0-9]{2}-[0-9]{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])') THEN
            epoch_ms(try_strptime(left(value, 19), '%Y-%m-%dT%H:%M:%S'))
            - CASE substr(value, 20, 1)
                WHEN 'Z' THEN 0
                ELSE CASE substr(value, 20, 1) WHEN '-' THEN -1 ELSE 1 END
                    * (substr(value, 21, 2)::BIGINT * 60 + substr(value, 24, 2)::BIGINT) * 60000
              END
        ELSE unsigned_integer(value)
    END
);

CREATE TABLE session_parts AS
WITH earliest_events AS (
    SELECT user_id, ts, ingest_ts, event_type, product_id, price_cents
    FROM input_events
    WHERE event_id IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
), parsed AS (
    SELECT user_id, event_time(ts) AS ts, epoch_ms(ingest_ts::TIMESTAMP) AS ingest_ms, product_id,
           event_type, CASE WHEN event_type = 'purchase' THEN unsigned_integer(price_cents) END AS price
    FROM earliest_events
    WHERE user_id IS NOT NULL AND product_id IS NOT NULL AND event_type IN ('view', 'cart', 'purchase')
), valid_events AS (
    SELECT coalesce(people.person, parsed.user_id) AS person, ts, product_id, price
    FROM parsed CROSS JOIN bounds LEFT JOIN people ON parsed.user_id = people.alias
    WHERE ts >= window_start AND ts < window_end AND ingest_ms - ts <= 86400000
      AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000)
), gaps AS (
    SELECT *, ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) AS gap
    FROM valid_events
), sessionized AS (
    SELECT person, product_id, price,
           max(CASE WHEN gap IS NULL OR gap > 1800000 THEN ts END)
               OVER (PARTITION BY person ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
               AS session_start
    FROM gaps
)
SELECT person, session_start, make_timestamp_ms(session_start)::DATE AS day,
       CASE WHEN price IS NOT NULL THEN coalesce(products.category, 'unknown') END AS category,
       count(*) AS events, coalesce(sum(price), 0)::BIGINT AS revenue
FROM sessionized LEFT JOIN input_products AS products USING (product_id)
GROUP BY 1, 2, 3, 4;

CREATE TABLE sessions AS
SELECT person, session_start, day, sum(events)::BIGINT AS events, sum(revenue)::BIGINT AS revenue
FROM session_parts
GROUP BY person, session_start, day;

CREATE TABLE user_daily AS
WITH category_totals AS (
    SELECT person, day, category, sum(revenue)::BIGINT AS revenue
    FROM session_parts
    WHERE category IS NOT NULL
    GROUP BY person, day, category
), top_categories AS (
    SELECT person, day,
           arg_min(category, struct_pack(revenue_order := -revenue, category := category)) AS top_category
    FROM category_totals
    GROUP BY person, day
), daily AS (
    SELECT person, day, count(*) AS sessions, sum(events)::BIGINT AS events,
           sum(revenue)::BIGINT AS revenue_cents
    FROM sessions
    GROUP BY person, day
)
SELECT person AS user_id, day, sessions, events, revenue_cents, top_category
FROM daily LEFT JOIN top_categories USING (person, day);

DROP TABLE session_parts;

CREATE TABLE valid_clicks AS
WITH parsed AS (
    SELECT click_id, user_id, campaign_id, event_time(ts) AS ts
    FROM input_clicks
    WHERE click_id IS NOT NULL AND user_id IS NOT NULL
)
-- Copies of a click have identical values, so deduplication and validation commute.
SELECT DISTINCT click_id, coalesce(people.person, parsed.user_id) AS person, campaign_id, ts
FROM parsed JOIN input_campaigns USING (campaign_id)
CROSS JOIN bounds LEFT JOIN people ON parsed.user_id = people.alias
WHERE ts >= window_start - 604800000 AND ts < window_end;

DROP TABLE people;

-- Reduce to one row per assisting campaign before selecting the credited click.
CREATE TABLE touches AS
SELECT s.person, s.session_start, s.day, c.campaign_id,
       min(struct_pack(time_order := -c.ts, click_id := c.click_id)) AS touch_order
FROM sessions AS s JOIN valid_clicks AS c
  ON s.person = c.person AND c.ts BETWEEN s.session_start - 604800000 AND s.session_start
GROUP BY s.person, s.session_start, s.day, c.campaign_id;

DROP TABLE valid_clicks;

CREATE TABLE campaign_daily AS
WITH winners AS (
    SELECT person, session_start, arg_min(campaign_id, touch_order) AS campaign_id
    FROM touches
    GROUP BY person, session_start
), credited AS (
    SELECT coalesce(winners.campaign_id, 'organic') AS campaign_id, day,
           count(*) AS sessions, count(*) FILTER (WHERE revenue > 0) AS purchasing_sessions,
           sum(revenue)::BIGINT AS revenue_cents
    FROM sessions LEFT JOIN winners USING (person, session_start)
    GROUP BY 1, day
), assisted AS (
    SELECT campaign_id, day, count(*) AS assisted_sessions
    FROM touches
    GROUP BY campaign_id, day
)
SELECT campaign_id, coalesce(campaigns.channel, 'organic') AS channel, day,
       coalesce(credited.sessions, 0) AS sessions,
       coalesce(credited.purchasing_sessions, 0) AS purchasing_sessions,
       coalesce(credited.revenue_cents, 0) AS revenue_cents,
       coalesce(assisted.assisted_sessions, 0) AS assisted_sessions
FROM credited FULL JOIN assisted USING (campaign_id, day)
LEFT JOIN input_campaigns AS campaigns USING (campaign_id);
