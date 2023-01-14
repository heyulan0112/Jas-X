//
//  metadata.cpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#include "metadata.hpp"
#include<iostream>
#include <cstring>
#include <vector>

using namespace hsql;

namespace jasdb{
    MetaData global_meta_data;
    std::vector<ColumnDefinition*> columns_;

    Table::Table(std::string schema, std::string name,std::vector<ColumnDefinition*>* columns){
        std::string schema_dup = schema;
        std::string name_dup = name;
        for(auto col : *columns){
            std::vector<ConstraintType>* constraints = new std::vector<ConstraintType>();
            *constraints = *col->column_constraints;
            std::string col_name_dup = col->name;
            ColumnDefinition* col = new ColumnDefinition(col_name_dup,col->type,constraints);
            col->nullable = col->nullable;
            columns_.push_back(col);
        }
        tableStore_ = new TableStore(&columns_);
    }
    Table::~Table(){
        
    }
    ColumnDefinition* Table::get_column(std::string name){
        if(name.size() == 0){
            return NULL;
        }
        for(auto col:columns_){
            if(name == col->name){
                return col;
            }
        }
        return NULL;
    }
    
    Index* Table::get_index(std::string name){
        if(name.size() == 0){
            return NULL;
        }
        for(auto index:indexes_){
            if(name == index->name){
                return index;
            }
        }
        return NULL;
    }
    
    bool MetaData::insert_table(Table *table){
        if(get_table(table->schema,table->name) != NULL){
            // already have this table
            return true;
        }
        else{
            TableName table_name;
            set_table_name(table_name, table->schema, table->name);
            map_of_table.emplace(table_name,table);
            return false;
        }
    }
    bool MetaData::drop_index(std::string name, std::string schema, std::string index_name){
        // get table pointer from shcema based on table name
        Table* table = get_table(schema,name);
        // no this table
        if(table == NULL){
            std::cout<<"Table is not exists"<<std::endl;
            return true;
        }
        bool res = true;
        for(auto i=0;i<table->indexes.size();i++){
            Index* index = table->indexes[i];
            if(index->name == index_name){
                table->indexes.erase(table->indexes.begin()+i);
                res = false;
            }
        }
        return res;
    }

    bool MetaData::drop_table(std::string schema, std::string name){
        Table* table = get_table(schema, name);
        if(table == NULL){
            std::cout<<"Table is not exists"<<std::endl;
            return true;
        }
        // construct the key which need to be removed from map_of_table from MetaData
        TableName table_name;
        
        // because wo have many shcema, thus we want to indentify a table we need to use schema+name
        set_table_name(table_name, schema, name);
        map_of_table.erase(table_name);
        
        //free this table space, delete [pointer] is freeing space this pointer pointing to
        delete table;
        return false;
    }

    bool MetaData::drop_schema(std::string schema){
        //delete all tables in this schema
        auto iterator = map_of_table.begin();
        bool res = true;
        while(iterator != map_of_table.end()){
            Table* table = iterator->second;
            if(table->schema == schema){
                // this table in this schema so need to be deleted
                std::cout<<"Drop table " << table->name << " in schema " << schema << std::endl;
                iterator = map_of_table.erase(iterator);
                delete table;
                res = false;
            }
            else{
                iterator++;
            }
        }
        return res;
    }
    
    void MetaData::get_all_tables(std::vector<Table*>* tables){
        // para is an empty vector
        // and need to add tables into it
        if(tables == NULL){
            return;
        }
        for(auto it:map_of_table){
            tables->push_back(it.second);
        }
    }

    bool MetaData::find_schema(std::string schema){
        for(auto it:map_of_table){
            Table* table = it.second;
            if(table->schema == schema){
                return true;
            }
        }
        return false;
    }

    Table* MetaData::get_table(std::string schema, std::string name){
        if(schema.size()==0 || name.size()==0){
            std::cout << "Need to specify schema and table name" << std::endl;
            return NULL;
        }
        // construct key
        TableName table_name;
        set_table_name(table_name, schema, name);
        auto it = map_of_table.find(table_name);
        if(it == map_of_table.end()){
            return NULL;
        }
        else{
            return it->second;
        }
    }

    Index* MetaData::get_index(std::string schema, std::string name, std::string index_name){
        Table* table = get_table(schema, name);
        if(table == NULL){
            std::cout << "Table not exist." << std::endl;
            return NULL;
        }
        for(auto index : table->indexes){
            if(index->name == index_name){
                return index;
            }
        }
        return NULL;
    }
}

