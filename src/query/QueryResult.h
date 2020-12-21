//
// Created by liu on 18-10-25.
//

#ifndef PROJECT_QUERYRESULT_H
#define PROJECT_QUERYRESULT_H

#include "../utils/formatter.h"

#include <memory>
#include <sstream>
#include <string>
#include <vector>

class QueryResult {
public:
    typedef std::unique_ptr<QueryResult> Ptr;

    virtual bool success() = 0;

    virtual std::string toString() = 0;

    virtual ~QueryResult() = default;
};

class FailedQueryResult : public QueryResult {
    const std::string data;
public:
    bool success() override { return false; }
};

class SuceededQueryResult : public QueryResult {
public:
    bool success() override { return true; }
};

class NullQueryResult : public SuceededQueryResult {
public:
    std::string toString() override {
        return std::string();
    }
};

class ErrorMsgResult : public FailedQueryResult {
    std::string msg;
public:
    ErrorMsgResult(const char *qname,
                   const std::string &msg) {
        this->msg = R"(Query "?" failed : ?)"_f % qname % msg;
    }

    ErrorMsgResult(const char *qname,
                   const std::string &table,
                   const std::string &msg) {
        this->msg =
                R"(Query "?" failed in Table "?" : ?)"_f % qname % table % msg;
    }

    std::string toString() override {
        return msg;
    }
};

class SuccessMsgResult : public SuceededQueryResult {
    std::string msg;
public:

    explicit SuccessMsgResult(const int number) {
        this->msg = R"(Answer = "?".)"_f % number;
    }

    explicit SuccessMsgResult(std::vector<int> results) {
        std::stringstream ss;
        ss << "Answer = ( ";
        for (auto result : results) {
            ss << result << " ";
        }
        ss << ")";
        this->msg = ss.str();
    }

    explicit SuccessMsgResult(const char *qname) {
        this->msg = R"(Query "?" success.)"_f % qname;
    }

    SuccessMsgResult(const char *qname, const std::string &msg) {
        this->msg = R"(Query "?" success : ?)"_f % qname % msg;
    }

    SuccessMsgResult(const char *qname,
                     const std::string &table,
                     const std::string &msg) {
        this->msg = R"(Query "?" success in Table "?" : ?)"_f
                    % qname % table % msg;
    }

    std::string toString() override {
        return msg;
    }
};

class RecordCountResult : public SuceededQueryResult {
    const int affectedRows;
public:
    explicit RecordCountResult(int count) : affectedRows(count) {}

    std::string toString() override {
        return "Affected ? rows."_f % affectedRows;
    }
};

#endif //PROJECT_QUERYRESULT_H
