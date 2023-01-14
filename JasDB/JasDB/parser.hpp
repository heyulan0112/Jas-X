//
//  parser.hpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#ifndef parser_hpp
#define parser_hpp

#include <stdio.h>
#include "sql-parser-master/src/SQLParser.h"
#include "sql-parser-master/src/SQLParserResult.h"
#include "sql-parser-master/src/util/sqlhelper.h"
#include "metadata.hpp"

using namespace hsql;

namespace jasdb{
    class Parser{
    public:
        SQLParserResult* result_;
        Parser();
        ~Parser();
        bool parseStatement(std::string query);
        SQLParserResult* get_result(){
            return result_;
        }
        bool check_stmts_meta();
        bool check_meta(const SQLStatement* stmt);
        bool check_select_stmt(const SelectStatement* stmt);
        bool check_insert_stmt(const InsertStatement* stmt);
        bool check_update_stmt(const UpdateStatement* stmt);
        bool check_delete_stmt(const DeleteStatement* stmt);
        bool check_create_stmt(const CreateStatement* stmt);
        bool check_drop_stmt(const DropStatement* stmt);
        bool check_create_index_stmt(const CreateStatement* stmt);
        bool check_create_table_stmt(const CreateStatement* stmt);
        Table* get_table(TableRef* table_ref);
        bool check_column(Table* table, std::string column_name);
        bool check_expression(Table* table, Expr* expr);
        bool check_values(std::vector<ColumnDefinition*> columns,std::vector<Expr*>* values);
    };
}

#endif /* parser_hpp */
