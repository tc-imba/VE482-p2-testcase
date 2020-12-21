//
// Created by liu on 18-10-23.
//

#include "Database.h"
#include "Table.h"

#include <sstream>
#include <iostream>
#include <iomanip>
#include <deque>

constexpr const Table::ValueType Table::ValueTypeMax;
constexpr const Table::ValueType Table::ValueTypeMin;

template<class FieldIDContainer>
Table::Table(const std::string &name, const FieldIDContainer &fields)
        : fields(fields.cbegin(), fields.cend()), tableName(name) {
    SizeType i = 0;
    for (const auto &field : fields) {
        if (field == "KEY")
            throw MultipleKey(
                    "Error creating table \"" + name + "\": Multiple KEY field."
            );
        fieldMap.emplace(field, i++);
    }
}

Table::FieldIndex Table::getFieldIndex(const Table::FieldNameType &field) const {
    try {
        return this->fieldMap.at(field);
    } catch (const std::out_of_range &e) {
        throw TableFieldNotFound(
                R"(Field name "?" doesn't exists.)"_f % (field)
        );
    }
}

void Table::insertByIndex(KeyType key, std::vector<ValueType> &&data) {
    if (this->keyMap.find(key) != this->keyMap.end()) {
        std::string err = "In Table \"" + this->tableName
                          + "\" : Key \"" + key + "\" already exists!";
        throw ConflictingKey(err);
    }
    this->keyMap.emplace(key, this->data.size());
    this->data.emplace_back(std::move(key), data);
}

Table::Object::Ptr Table::operator[](const Table::KeyType &key) {
    auto it = keyMap.find(key);
    if (it == keyMap.end()) {
        // not found
        return nullptr;
    } else {
        return createProxy(data.begin() + it->second, this);
    }
}

std::ostream &operator<<(std::ostream &os, const Table &table) {
    const int width = 10;
    std::stringstream buffer;
    buffer << table.tableName << "\t" << (table.fields.size() + 1) << "\n";
    buffer << std::setw(width) << "KEY";
    for (const auto &field : table.fields) {
        buffer << std::setw(width) << field;
    }
    buffer << "\n";
    auto numFields = table.fields.size();
    for (const auto &datum : table.data) {
        buffer << std::setw(width) << datum.key;
        for (decltype(numFields) i = 0; i < numFields; ++i) {
            buffer << std::setw(width) << datum.datum[i];
        }
        buffer << "\n";
    }
    return os << buffer.str();
}

Table &loadTableFromStream(std::istream &is, std::string source) {
    auto &db = Database::getInstance();
    std::string errString =
            !source.empty() ?
            R"(Invalid table (from "?") format: )"_f % source :
            "Invalid table format: ";

    std::string tableName;
    Table::SizeType fieldCount;
    std::deque<Table::KeyType> fields;

    std::string line;
    std::stringstream sstream;
    if (!std::getline(is, line))
        throw LoadFromStreamException(
                errString + "Failed to read table metadata line."
        );

    sstream.str(line);
    sstream >> tableName >> fieldCount;
    if (!sstream) {
        throw LoadFromStreamException(
                errString + "Failed to parse table metadata."
        );
    }

    // throw error if tableName duplicates
    db.testDuplicate(tableName);

    if (!(std::getline(is, line))) {
        throw LoadFromStreamException(
                errString + "Failed to load field names."
        );
    }

    sstream.clear();
    sstream.str(line);
    for (Table::SizeType i = 0; i < fieldCount; ++i) {
        std::string field;
        if (!(sstream >> field)) {
            throw LoadFromStreamException(
                    errString + "Failed to load field names."
            );
        }
        else {
            fields.emplace_back(std::move(field));
        }
    }

    if (fields.front() != "KEY") {
        throw LoadFromStreamException(
                errString + "Missing or invalid KEY field."
        );
    }

    fields.erase(fields.begin()); // Remove leading key
    auto table = std::make_unique<Table>(tableName, fields);

    Table::SizeType lineCount = 2;
    while (std::getline(is, line)) {
        if (line.empty()) break; // Read to an empty line
        lineCount++;
        sstream.clear();
        sstream.str(line);
        std::string key;
        if (!(sstream >> key))
            throw LoadFromStreamException(
                    errString + "Missing or invalid KEY field."
            );
        std::vector<Table::ValueType> tuple;
        tuple.reserve(fieldCount - 1);
        for (Table::SizeType i = 1; i < fieldCount; ++i) {
            Table::ValueType value;
            if (!(sstream >> value))
                throw LoadFromStreamException(
                        errString + "Invalid row on LINE " + std::to_string(lineCount)
                );
            tuple.emplace_back(value);
        }
        table->insertByIndex(key, std::move(tuple));
    }

    return db.registerTable(std::move(table));
}
