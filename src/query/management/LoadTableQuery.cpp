//
// Created by liu on 18-10-25.
//

#include "LoadTableQuery.h"

#include <fstream>

constexpr const char *LoadTableQuery::qname;

std::string LoadTableQuery::toString() {
    return "QUERY = Load TABLE, FILE = \"" + this->fileName + "\"";
}

QueryResult::Ptr LoadTableQuery::execute() {
    try {
        std::ifstream infile(this->fileName);
        if (!infile.is_open()) {
            return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
        }
        loadTableFromStream(infile, this->fileName);
        infile.close();
    } catch (const std::exception &e) {
        return std::make_unique<ErrorMsgResult>(qname, e.what());
    }
    return std::make_unique<SuccessMsgResult>(qname);
}
