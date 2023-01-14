//
//  parser.cpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#include "parser.hpp"
#include "util.h"
#include <iostream>
using namespace hsql;

namespace jasdb{
    Parser::Parser(){
        result_ = NULL;
    }

    Parser::~Parser(){
        delete result_;
        result_ = NULL;
    }

    bool Parser::parseStatement(std::string query){
        result_ = new SQLParserResult();
        SQLParser::parse(query,result_);
        if(result_->isValid()){
            return check_stmts_meta();
        }
        else{
            std::cout<<"Fail to parse this sql statement."<<std::endl;
        }
        return true;
    }

    bool Parser::check_stmts_meta(){
        // use a for loop to check_meta for each stmt in result_
        // because one query may contain multi stmts
        for(auto i=0;i<result_->size();i++){
            const SQLStatement* stmt = result_->getStatement(i);
            // return true means not pass check
            if(check_meta(stmt)){
                return true;
            }
        }
        return false;
    }

    bool Parser::check_meta(const SQLStatement *stmt){
        switch (stmt->type()) {
            case kStmtSelect:
                return check_select_stmt(static_cast<const SelectStatement*>(stmt));
            case kStmtInsert:
                return check_insert_stmt(static_cast<const InsertStatement*>(stmt));
            case kStmtUpdate:
                return check_update_stmt(static_cast<const UpdateStatement*>(stmt));
            case kStmtDelete:
                return check_delete_stmt(static_cast<const DeleteStatement*>(stmt));
            case kStmtCreate:
                return check_create_stmt(static_cast<const CreateStatement*>(stmt));
            case kStmtDrop:
                return check_drop_stmt(static_cast<const DropStatement*>(stmt));
            case kStmtTransaction:
            case kStmtShow:
                return false;
            default:
                std::cout<<"This type statement is not support now."<<std::endl;
        }
        return true;
    }

    bool Parser::check_select_stmt(const SelectStatement *stmt){
        TableRef* table_ref = stmt->fromTable;
        Table* table = get_table(table_ref);
        if(table == NULL){
            std::cout<<"Table not found."<<std::endl;
            return true;
        }
        if(stmt->groupBy != NULL){
            std::cout<<"Not support group by."<<std::endl;
            return true;
        }
        if(stmt->setOperations != NULL){
            std::cout<<"Not support set operation."<<std::endl;
            return true;
        }
        if(stmt->withDescriptions != NULL){
            std::cout<<"Not support 'with' clause."<<std::endl;
            return true;
        }
        if(stmt->lockings != NULL){
            std::cout<<"Not support 'lock' clause."<<std::endl;
            return true;
        }
        if(stmt->selectList != NULL){
            for(auto expression : *stmt->selectList){
                if(check_expression(table, expression)){
                    return true;
                }
            }
        }
        if(stmt->whereClause != NULL){
            if(check_expression(table, stmt->whereClause)){
                return true;
            }
        }
        if(stmt->order != NULL){
            for(auto o:*stmt->order){
                if(check_expression(table, o->expr)){
                    return true;
                }
            }
        }
        if(stmt->limit != NULL){
            if(check_expression(table, stmt->limit->limit)){
                return true;
            }
            if(check_expression(table, stmt->limit->offset)){
                return true;
            }
        }
        return false;
    }

    bool Parser::check_insert_stmt(const InsertStatement *stmt){
        if(stmt->type == kInsertSelect){
            std::cout << "Do not support 'INSERT INTO ... SELECT ...' now."<< std::endl;
        }
        Table* table = global_meta_data.get_table(stmt->schema, stmt->tableName);
        if(table == NULL){
            std::cout<<"Table not found."<<std::endl;
            return true;
        }
        // check if this table have this column
        if(stmt->columns != NULL){
            for(auto col : *stmt->columns){
                if(check_column(table, col)){
                    return true;
                }
            }
        }
        std::vector<Expr*> inserted_values;
        for(auto i=0;i<table->columns.size();i++){
            auto col_def = table->columns[i];
            if(stmt->columns != NULL){
                auto j = 0;
                for(;j<stmt->columns->size();j++){
                    if(col_def->name == (*stmt->columns)[j]){
                        break;
                    }
                }
                if(j<stmt->columns->size()){
                    inserted_values.push_back((*stmt->values)[j]);
                }
                else{
                    Expr* expr = new Expr(kExprLiteralNull);
                    inserted_values.push_back(expr);
                }
            }
            else{
                if(i<stmt->values->size()){
                    inserted_values.push_back((*stmt->values)[i]);
                }
                else{
                    Expr* expr = new Expr(kExprLiteralNull);
                    inserted_values.push_back(expr);
                }
            }
        }
        stmt->values->assign(inserted_values.begin(), inserted_values.end());
        
        // check if each value match column value constraints
        if(check_values(table->columns, stmt->values)){
            return true;
        }
        return false;
    }

    bool Parser::check_update_stmt(const UpdateStatement *stmt){
        TableRef* table_ref = stmt->table;
        Table* table = get_table(table_ref);
        if(table == NULL){
            std::cout<<"Table not found."<<std::endl;
            return true;
        }
        if(stmt->updates != NULL){
            for(auto update:*stmt->updates){
                if(check_column(table, update->column)){
                    return true;
                }
                if(check_expression(table, update->value)){
                    return true;
                }
            }
        }
        if(check_expression(table, stmt->where)){
            return true;
        }
        return false;
    }

    bool Parser::check_delete_stmt(const DeleteStatement *stmt){
        Table* table = global_meta_data.get_table(stmt->schema, stmt->tableName);
        if(table == NULL){
            std::cout<<"Table not found."<<std::endl;
            return true;
        }
        if(check_expression(table, stmt->expr)){
            return true;
        }
        return false;
    }

    Table* Parser::get_table(TableRef *table_ref){
        if(table_ref->type != kTableName){
            return NULL;
        }
        Table* table  = global_meta_data.get_table(table_ref->schema, table_ref->name);
        if(table == NULL){
            std::cout<<"Table not found."<<std::endl;
            return NULL;
        }
        return table;
    }
    
    bool Parser::check_column(Table *table, std::string column_name){
        for(auto col_def:table->columns){
            // if one column match then pass check
            if(column_name == col_def->name){
                return false;
            }
        }
        std::cout<<"No this column in table."<<std::endl;
        return true;
    }

    bool Parser::check_expression(Table *table, Expr *expr){
        switch(expr->type){
            case hsql::kExprLiteralNull:
            case hsql::kExprLiteralString:
            case kExprLiteralInt:
            case kExprStar:
                return false;
            case kExprSelect:
                return check_expression(table, expr->expr);
            case kExprOperator:
            {
                if(expr->expr != NULL && check_expression(table, expr->expr)){
                    return true;
                }
                if (expr->expr2 != NULL && check_expression(table, expr->expr2)) {
                    return true;
                }
                break;
            }
            case kExprColumnRef:
            {
                if(check_column(table, expr->name)){
                    return true;
                }
                break;
            }
            default:
                std::cout<<"This operation is not supported"<<std::endl;
                return true;
                
        }
        return false;
    }

    bool Parser::check_values(std::vector<ColumnDefinition*> columns, std::vector<Expr*>* values){
        for(auto i=0;i<columns.size();i++){
            auto col_def = columns[i];
            auto expr = (*values)[i];
            switch (col_def->type.data_type) {
                case DataType::INT:
                case DataType::LONG:
                {
                    if(expr->type != kExprLiteralInt){
                        std::cout<<"Incorrect data type"<<std::endl;
                        return true;
                    }
                    if(col_def->type.data_type == DataType::INT && expr->ival > INT_MAX){
                        std::cout<<"Data too large"<<std::endl;
                        return true;
                    }
                    break;
                }
                case DataType::CHAR:
                case DataType::VARCHAR:
                {
                    if(expr->type != kExprLiteralString){
                        std::cout<<"Incorrect data type"<<std::endl;
                        return true;
                    }
                    if(strlen(expr->name) > static_cast<size_t>(col_def->type.length)){
                        std::cout<<"Data too large for column"<<std::endl;
                        return true;
                    }
                    break;
                }
                default:
                    return true;
                    break;
            }
        }
        return false;
    }

    bool Parser::check_create_stmt(const CreateStatement *stmt){
        switch (stmt->type) {
            case kCreateTable:
            {
                if(check_create_table_stmt(stmt)){
                    return true;
                }
                break;
            }
            case kCreateIndex:
            {
                if(check_create_index_stmt(stmt)){
                    return true;
                }
                break;
            }
            default:
                return true;
        }
        return false;
    }

    bool Parser::check_create_table_stmt(const CreateStatement *stmt){
        if(stmt->schema == NULL || stmt->tableName == NULL){
            std::cout << "Need to specify schema and table name" << std::endl;
            return true;
        }
        
        if(global_meta_data.get_table(stmt->schema, stmt->tableName) != NULL && !stmt->ifNotExists){
            std::cout << "This table already existed." << std::endl;
            return true;
        }
        
        if(stmt->columns == NULL || stmt->columns->size() == 0){
            std::cout << "Need to specify valid columns." << std::endl;
            return true;
        }
        
        for(auto col_def : *stmt->columns){
            if(col_def == NULL || col_def->name == NULL){
                std::cout << "Need to specify valid columns." << std::endl;
                return true;
            }
            if(!IsDataTypeSupport(col_def->type.data_type)){
                std::cout << "Data type is not supported." << std::endl;
                return true;
            }
        }
        
        return false;
    }

    bool Parser::check_create_index_stmt(const CreateStatement *stmt){
        if(global_meta_data.get_index(stmt->schema, stmt->tableName, stmt->indexName) != NULL && !stmt->ifNotExists){
            std::cout << "This index already existed." << std::endl;
            return true;
        }
        // check if each column this index need exist or not
        Table* table = global_meta_data.get_table(stmt->schema, stmt->tableName);
        for(auto col : *stmt->indexColumns){
            if(check_column(table, col)){
                return true;
            }
        }
        return false;
    }

    bool Parser::check_drop_stmt(const DropStatement *stmt){
        switch (stmt->type) {
            case kDropTable:
            {
                if(global_meta_data.get_table(stmt->schema, stmt->name) == NULL && !stmt->ifExists){
                    std::cout << "Table not found." << std::endl;
                    return true;
                }
                break;
            }
            case kDropSchema:
            {
                if(global_meta_data.find_schema(stmt->schema) && !stmt->ifExists){
                    std::cout << "Schema not found." << std::endl;
                    return true;
                }
                break;
            }
            case kDropIndex:
            {
                if(global_meta_data.get_index(stmt->schema,stmt->name,stmt->indexName) == NULL && !stmt->ifExists){
                    std::cout << "Index not found." << std::endl;
                    return true;
                }
                break;
            }
            default:
                std::cout << "Not support this type of drop statement." << std::endl;
                return true;
        }
        return false;
    }
}
