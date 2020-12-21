//
// Created by liu on 18-10-25.
//

#include "InsertQuery.h"
#include "../../db/Database.h"
#include "../QueryResult.h"

#include <algorithm>

constexpr const char *InsertQuery::qname;

QueryResult::Ptr InsertQuery::execute() {
    using namespace std;
    if (this->operands.empty())
        return make_unique<ErrorMsgResult>(
                qname, this->targetTable.c_str(),
                "No operand (? operands)."_f % operands.size()
        );
    Database &db = Database::getInstance();
    try {
        auto &table = db[this->targetTable];
        auto &key = this->operands.front();
        vector<Table::ValueType> data;
        for_each(++this->operands.begin(), this->operands.end(), [&data](const string &item) {
            data.emplace_back(strtol(item.c_str(), nullptr, 10));
        });
        table.insertByIndex(key, move(data));
        return make_unique<NullQueryResult>();
    }
    catch (const TableNameNotFound &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "No such table."s);
    } catch (const IllFormedQueryCondition &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, e.what());
    } catch (const invalid_argument &e) {
        // Cannot convert operand to string
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unknown error '?'"_f % e.what());
    } catch (const exception &e) {
        return make_unique<ErrorMsgResult>(qname, this->targetTable, "Unkonwn error '?'."_f % e.what());
    }
}

std::string InsertQuery::toString() {
    return "QUERY = INSERT " + this->targetTable + "\"";
}
