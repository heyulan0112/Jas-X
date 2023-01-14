//
//  metadata.hpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#ifndef metadata_hpp
#define metadata_hpp

#include <stdio.h>
#include <cstring>
#include "sql-parser-master/src/sql/CreateStatement.h"
#include "sql-parser-master/src/sql/Table.h"
#include <unordered_map>

using namespace hsql;

namespace jasdb {
    class Index {
    public:
        std::string name;
        std::vector<ColumnDefinition*> columns;
        Index(){}
    };

    class Table {
    public:
        std::string schema;
        std::string name;
        std::vector<ColumnDefinition*> columns;
        std::vector<Index*> indexes;
        TableStore* table_store;
        Table(std::string schema, std::string name,std::vector<ColumnDefinition*>* columns);
        ~Table();
        ColumnDefinition* get_column(std::string name);
        Index* get_index(std::string name);
        void add_index(Index* index){
            indexes.push_back(index);
        }
    };
    
    class MetaData{
    public:
        std::unordered_map<TableName, Table*> map_of_table;
        MetaData(){
            
        }
        ~MetaData(){
            
        }
        bool insert_table(Table* table);
        bool drop_table(std::string schema, std::string name);
        bool drop_schema(std::string schema);
        bool drop_index(std::string name, std::string schema, std::string index_name);
        void get_all_tables(std::vector<Table*>* tables);
        bool find_schema(std::string schema);
        Table* get_table(std::string schema, std::string name);
        Index* get_index(std::string schema, std::string name, std::string index_name);
        void set_table_name(TableName table_name,std::string schema, std::string name){};
    };
    extern MetaData global_meta_data;
}

#endif /* metadata_hpp */
