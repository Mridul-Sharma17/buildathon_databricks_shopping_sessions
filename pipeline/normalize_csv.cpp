#include <array>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

struct Field {
    const char* data;
    std::size_t size;
};

// The contest CSV dialect allows quotes only around a complete field.
bool parse_row(const std::string& line, Field* fields, int count) {
    std::size_t start = 0;
    for (int i = 0; i < count; ++i) {
        const auto comma = line.find(',', start);
        if ((comma == std::string::npos) != (i == count - 1)) {
            return false;
        }
        const auto end = comma == std::string::npos ? line.size() : comma;
        Field value{line.data() + start, end - start};
        if (value.size != 0 && value.data[0] == '"') {
            if (value.size < 2 || value.data[value.size - 1] != '"') {
                return false;
            }
            ++value.data;
            value.size -= 2;
        }
        if (std::memchr(value.data, '"', value.size)) {
            return false;
        }
        fields[i] = value;
        start = end + 1;
    }
    return true;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr << "usage: normalize_csv INPUT OUTPUT FIELD_COUNT\n";
        return 1;
    }
    const int count = std::stoi(argv[3]);
    std::array<Field, 7> fields;
    if (count < 2 || count > static_cast<int>(fields.size())) {
        return 1;
    }

    std::ifstream input(argv[1], std::ios::binary);
    std::ofstream output(argv[2], std::ios::binary);
    std::string line;
    if (!std::getline(input, line) || !output) {
        std::cerr << "cannot open CSV files for " << argv[1] << '\n';
        return 1;
    }
    output << line << '\n';

    std::string normalized;
    while (std::getline(input, line)) {
        if (!parse_row(line, fields.data(), count)) {
            continue;
        }
        normalized.clear();
        for (int i = 0; i < count; ++i) {
            if (i != 0) {
                normalized += ',';
            }
            const auto value = fields[i];
            const bool quote = std::memchr(value.data, '\r', value.size) != nullptr;
            if (quote) {
                normalized += '"';
            }
            normalized.append(value.data, value.size);
            if (quote) {
                normalized += '"';
            }
        }
        normalized += '\n';
        output << normalized;
    }
    output.close();
    if (!input.eof() || input.bad() || !output) {
        std::cerr << "CSV I/O failed for " << argv[1] << '\n';
        return 1;
    }
}
