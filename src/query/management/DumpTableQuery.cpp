//
// Created by liu on 18-10-25.
//

#include "DumpTableQuery.h"
#include "../../db/Database.h"

#include <fstream>

constexpr const char *DumpTableQuery::qname;

std::string DumpTableQuery::toString() {
    return "QUERY = Dump TABLE, FILE = \"" + this->fileName + "\"";
}

QueryResult::Ptr DumpTableQuery::execute() {
    try {
        std::ofstream outfile(this->fileName);
        if (!outfile.is_open()) {
            return std::make_unique<ErrorMsgResult>(qname, "Cannot open file '?'"_f % this->fileName);
        }
        auto &db = Database::getInstance();
        outfile << db[this->targetTable];
        outfile.close();
    } catch (const std::exception &e) {
        return std::make_unique<ErrorMsgResult>(qname, e.what());
    }
    return std::make_unique<SuccessMsgResult>(qname);
}
