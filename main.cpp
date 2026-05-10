#include <iostream>
#include <string>
#include <algorithm>
#include <cctype>
#include "parser.h"
#include "db.h"

void printWelcome() {
    std::cout << "\n================================================================================" << std::endl;
    std::cout << "                    CUSTOM DATABASE MANAGEMENT SYSTEM" << std::endl;
    std::cout << "================================================================================" << std::endl;
    std::cout << "\nDatabase Commands:" << std::endl;
    std::cout << "  CREATE DATABASE <name>;              - Create a new database" << std::endl;
    std::cout << "  DROP DATABASE <name>;                - Delete a database" << std::endl;
    std::cout << "  SHOW DATABASES;                      - List all databases" << std::endl;
    std::cout << "  USE <database>;                      - Switch to a database" << std::endl;
    std::cout << "  SHOW TABLES;                         - List tables in current database" << std::endl;
    std::cout << "  DESC[RIBE] <table>;                  - Show table structure" << std::endl;
    std::cout << "\nTable Commands (DDL):" << std::endl;
    std::cout << "  CREATE TABLE <name> (col1 TYPE constraints, ...);" << std::endl;
    std::cout << "  DROP TABLE <name>;                   - Delete a table" << std::endl;
    std::cout << "  CREATE INDEX <name> ON <table>(col); - Create an index" << std::endl;
    std::cout << "\nData Commands (DML):" << std::endl;
    std::cout << "  INSERT INTO <table> VALUES (val1, val2, ...);" << std::endl;
    std::cout << "  SELECT * FROM <table> [WHERE condition] [ORDER BY col ASC|DESC];" << std::endl;
    std::cout << "  UPDATE <table> SET col=value [WHERE condition];" << std::endl;
    std::cout << "  DELETE FROM <table> [WHERE condition];" << std::endl;
    std::cout << "\nAdvanced Features:" << std::endl;
    std::cout << "  SELECT cols FROM t1 JOIN t2 ON t1.col = t2.col;" << std::endl;
    std::cout << "  SELECT SUM(col), AVG(col), COUNT(col) FROM table [WHERE ...];" << std::endl;
    std::cout << "  EXPORT TO CSV <file> FROM <table>;   - Export to CSV" << std::endl;
    std::cout << "  BACKUP DATABASE TO <name>;           - Backup database" << std::endl;
    std::cout << "  RESTORE DATABASE FROM <name>;        - Restore from backup" << std::endl;
    std::cout << "\nTransaction Commands:" << std::endl;
    std::cout << "  BEGIN;                               - Start transaction" << std::endl;
    std::cout << "  COMMIT;                              - Commit transaction" << std::endl;
    std::cout << "  ROLLBACK;                            - Rollback transaction" << std::endl;
    std::cout << "\nOther:" << std::endl;
    std::cout << "  EXIT;                                - Exit the program" << std::endl;
    std::cout << "================================================================================" << std::endl;
}

int main() {
    loadDatabaseList();
    printWelcome();
    
    std::string input;
    bool running = true;
    
    while (running) {
        std::cout << "\n" << (currentDatabase.empty() ? "dbms" : currentDatabase) << "> ";
        std::getline(std::cin, input);
        
        if (input.empty()) continue;
        
        std::string upperInput = input;
        std::transform(upperInput.begin(), upperInput.end(), upperInput.begin(), ::toupper);
        
        if (upperInput == "EXIT" || upperInput == "QUIT" || upperInput == "EXIT;") {
            // Save current database before exiting
            if (!currentDatabase.empty()) {
                std::cout << "Saving database '" << currentDatabase << "'..." << std::endl;
                saveToFile(currentDatabase);
                std::cout << "Database saved successfully!" << std::endl;
            }
            std::cout << "\nGoodbye!" << std::endl;
            running = false;
            break;
        }
        
        try {
            if (!parseAndExecute(input)) {
                // Command failed, continue
            }
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }
    
    return 0;
}