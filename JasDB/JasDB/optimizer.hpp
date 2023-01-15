//
//  optimizer.hpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#ifndef optimizer_hpp
#define optimizer_hpp

#include <stdio.h>
#include <cstring>
#include "sql-parser-master/src/sql/statements.h"
#include "metadata.hpp"
using namespace hsql;

namespace jasdb{
enum planType{
    kCreate,
    kDrop,
    kInsert,
    kUpdate,
    kDelete,
    kSelect,
    kScan,
    kProjection,
    kFilter,
    kSort,
    kLimit,
    kTrx,
    kShow
};

class Plan {
public:
    planType plan_type;
    Plan* next;
    Plan(planType t){
        plan_type = t;
        next = NULL;
    }
    ~Plan(){
        delete next;
        next = NULL;
    }
};

class CreatePlan : public Plan{
public:
    CreateType type; // to specify what kind of create create index table or schema
    bool ifNotExist;
    char* schema;
    char* tableName;
    char* indexName;
    std::vector<ColumnDefinition*>* indexColumns;
    std::vector<ColumnDefinition*>* columns;
    CreatePlan(CreateType t) : Plan(kCreate){
        type = t;
    }
};

class DropPlan : public Plan{
public:
    DropType type;
    bool ifExists;
    char* schema;
    char* name;
    char* indexName;
    DropPlan() : Plan(kDrop){}
};

class InsertPlan : public Plan {
public:
    InsertType type;
    Table* table;
    std::vector<Expr*>* values;
    InsertPlan() : Plan(kInsert){}
};

class UpdatePlan : public Plan {
public:
    Table* table;
    std::vector<Expr*> values;
    std::vector<size_t> indexes;
    UpdatePlan() : Plan(kUpdate){}
};

class DeletePlan : public Plan {
public:
    Table* table;
    DeletePlan() : Plan(kDelete){}
};

class SelectPlan : public Plan {
public:
    Table* table;
    std::vector<ColumnDefinition*> out_cols;
    std::vector<size_t> col_ids;
    SelectPlan() : Plan(kSelect) {}
};

enum scanType {
    kSeqScan,
    kIndexScan
};

class ScanPlan : public Plan {
public:
    scanType type;
    Table* table;
    ScanPlan() : Plan(kScan){}
};

class FilterPlan : public Plan {
public:
    size_t idx;
    Expr* val;
    FilterPlan() : Plan(kFilter), idx(0), val(NULL){}
};

class SortPlan : public Plan {
public:
    Table* table;
    std::vector<OrderDescription*> order;
    SortPlan() : Plan(kSort){}
};

class LimitPlan : public Plan {
public:
    LimitPlan() : Plan(kLimit) {}
    int offset;
    int limit;
};

class TrxPlan : public Plan {
public:
    TrxPlan() : Plan(kTrx){}
    TransactionCommand command;
};

class ShowPlan : public Plan {
public:
    ShowType type;
    std::string schema;
    std::string name;
    ShowPlan() : Plan(kShow){}
};

class Optimizer {
public:
    Optimizer(){}
    Plan* create_plan_tree(const SQLStatement* stmt);
    Plan* create_create_plan_tree(const CreateStatement* stmt);
    Plan* create_drop_plan_tree(const DropStatement* stmt);
    Plan* create_insert_plan_tree(const InsertStatement* stmt);
    Plan* create_update_plan_tree(const UpdateStatement* stmt);
    Plan* create_delete_plan_tree(const DeleteStatement* stmt);
    Plan* create_select_plan_tree(const SelectStatement* stmt);
    Plan* create_filter_plan_tree(std::vector<ColumnDefinition*> columns, Expr* where);
    Plan* create_trx_plan_tree(const TransactionStatement* stmt);
    Plan* create_show_plan_tree(const ShowStatement* stmt);
};

}

#endif /* optimizer_hpp */
