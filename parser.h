#ifndef PARSER_H
#define PARSER_H

#include <string>
#include <vector>
#include "global.h"

bool parseAndExecute(const std::string& query);
bool parseCreateDatabase(const std::string& query);
bool parseDropDatabase(const std::string& query);
bool parseShowDatabases(const std::string& query);
bool parseUse(const std::string& query);
bool parseCreateTable(const std::string& query);
bool parseInsert(const std::string& query);
bool parseSelect(const std::string& query);
bool parseUpdate(const std::string& query);
bool parseDelete(const std::string& query);
bool parseCreateIndex(const std::string& query);
bool parseDropTable(const std::string& query);
bool parseShowTables(const std::string& query);
bool parseDescribe(const std::string& query);
bool parseJoin(const std::string& query);
bool parseAggregate(const std::string& query);
bool parseExport(const std::string& query);
bool parseBackup(const std::string& query);
bool parseRestore(const std::string& query);
bool parseTransaction(const std::string& query);

#endif