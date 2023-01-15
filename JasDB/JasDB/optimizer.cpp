//
//  optimizer.cpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#include "optimizer.hpp"
#include <iostream>
using namespace hsql;

namespace jasdb{

Plan* Optimizer::create_plan_tree(const SQLStatement* stmt){
    switch (stmt->type()) {
        case kStmtSelect:
            return create_select_plan_tree(static_cast<const SelectStatement*>(stmt));
        case kStmtInsert:
            return create_insert_plan_tree(static_cast<const InsertStatement*>(stmt));
        case kStmtUpdate:
            return create_update_plan_tree(static_cast<const UpdateStatement*>(stmt));
        case kStmtDelete:
            return create_delete_plan_tree(static_cast<const DeleteStatement*>(stmt));
        case kStmtCreate:
            return create_create_plan_tree(static_cast<const CreateStatement*>(stmt));
        case kStmtDrop:
            return create_drop_plan_tree(static_cast<const DropStatement*>(stmt));
        case kStmtTransaction:
            return create_trx_plan_tree(static_cast<const TransactionStatement*>(stmt));
        case kStmtShow:
            return create_show_plan_tree(static_cast<const ShowStatement*>(stmt));
        default:
            std::cout << "Statement type is not supported." << std::endl;
    }
    return NULL;
}

Plan* Optimizer::create_create_plan_tree(const CreateStatement *stmt){
    CreatePlan* plan = new CreatePlan(stmt->type);
    plan->ifNotExist = stmt->ifNotExists;
    plan->type = stmt->type;
    plan->schema = stmt->schema;
    plan->tableName = stmt->tableName;
    plan->indexName = stmt->indexName;
    plan->columns = stmt->columns;
    plan->next = NULL;
    
    // Create index for an existed table
    if(plan->type == kCreateIndex){
        Table* table = global_meta_data.get_table(plan->schema, plan->tableName);
        if(table == NULL){
            std::cout << "Table not found." << std::endl;
            delete plan;
            return NULL;
        }
        if(stmt->indexColumns != NULL){
            plan->indexColumns = new std::vector<ColumnDefinition*>;
        }
        for(auto column_name : *stmt->indexColumns){
            ColumnDefinition* col_def = table->get_column(column_name);
            if(col_def == NULL){
                std::cout << "Column not found." << std::endl;
                delete plan->indexColumns;
                delete plan;
                return NULL;
            }
            plan->indexColumns->push_back(col_def);
        }
        return plan;
    }
    
    return plan;
}

Plan* Optimizer::create_drop_plan_tree(const DropStatement *stmt){
    DropPlan* plan = new DropPlan();
    plan->type = stmt->type;
    plan->ifExists = stmt->ifExists;
    plan->schema = stmt->schema;
    plan->name = stmt->name;
    plan->indexName = stmt->indexName;
    plan->next = NULL;
    return plan;
}

Plan* Optimizer::create_insert_plan_tree(const InsertStatement *stmt){
    InsertPlan* plan = new InsertPlan();
    plan->type = stmt->type;
    plan->table = global_meta_data.get_table(stmt->schema, stmt->tableName);
    plan->values = stmt->values;
    return plan;
}

Plan* Optimizer::create_update_plan_tree(const UpdateStatement *stmt){
    Table* table = global_meta_data.get_table(stmt->table->schema, stmt->table->name);
    ScanPlan* scan = new ScanPlan();
    scan->type = kSeqScan;
    scan->table = table;
    Plan* plan = scan;
    if(stmt->where != NULL){
        Plan* filter = create_filter_plan_tree(table->columns, stmt->where);
        filter->next = plan;
        plan = filter;
    }
    UpdatePlan* update_plan = new UpdatePlan();
    update_plan->table = table;
    // Update->Filter->Scan
    update_plan->next = plan;

    for(auto update : *stmt->updates){
        size_t idx = 0;
        update_plan->values.push_back(update->value);
        for(auto col : table->columns){
            // char* string
            if(update->column == col->name){
                // which index to be updated
                update_plan->indexes.push_back(idx);
                break;
            }
            idx++;
        }
    }
    
    return update_plan;
}

Plan* Optimizer::create_delete_plan_tree(const DeleteStatement *stmt){
    Table* table = global_meta_data.get_table(stmt->schema, stmt->tableName);
    
    ScanPlan* scan = new ScanPlan();
    scan->type = kSeqScan;
    scan->table = table;
    Plan* plan = scan;
    
    if(stmt->expr != NULL){
        Plan* filter = create_filter_plan_tree(table->columns, stmt->expr);
        filter->next = plan;
        plan = filter;
    }
    
    DeletePlan* del_plan = new DeletePlan();
    del_plan->table = table;
    // Delete->Filter->Scan
    del_plan->next = plan;
    return del_plan;
}

Plan* Optimizer::create_select_plan_tree(const SelectStatement *stmt){
    Table* table = global_meta_data.get_table(stmt->fromTable->schema, stmt->fromTable->name);
    std::vector<ColumnDefinition*> columns = table->columns;
    ScanPlan* scan = new ScanPlan();
    scan->type = kSeqScan;
    scan->table = table;
    Plan* plan = scan;
    if(stmt->whereClause != NULL){
        Plan* filter = create_filter_plan_tree(columns, stmt->whereClause);
        filter->next = plan;
        plan = filter;
    }
    SelectPlan* select_plan = new SelectPlan();
    select_plan->table = table;
    // Select->Filter->Scan
    select_plan->next = plan;
    
    for(auto expr:*stmt->selectList){
        if(expr->type == kExprStar){
            // select *
            for(size_t i=0;i<columns.size();i++){
                ColumnDefinition* col_def = columns[i];
                select_plan->out_cols.push_back(col_def);
                select_plan->col_ids.push_back(i);
            }
        }
        else{
            for(size_t i=0;i<columns.size();i++){
                // select col_name1, col_name2, ...
                ColumnDefinition* col_def = columns[i];
                if(strcmp(expr->name, col_def->name) == 0){
                    select_plan->out_cols.push_back(col_def);
                    select_plan->col_ids.push_back(i);
                }
            }
        }
    }
    return select_plan;
}

// used to 'where' like "where id = 1"
// FilterPlan contains the filter information
Plan* Optimizer::create_filter_plan_tree(std::vector<ColumnDefinition *> columns, Expr *where){
    FilterPlan* filter = new FilterPlan();
    Expr* col = NULL;
    Expr* val = NULL;
    if(where->expr->type == kExprColumnRef){
        col = where->expr;
        val = where->expr2;
    }
    else{
        col = where->expr2;
        val = where->expr;
    }
    // get the index of column which need to be filtered out
    for(auto i=0;i<columns.size();i++){
        ColumnDefinition* col_def = columns[i];
        if(strcmp(col->name, col_def->name) == 0){
            filter->idx = i;
        }
    }
    filter->val = val;
    return filter;
}

Plan* Optimizer::create_trx_plan_tree(const TransactionStatement *stmt){
    TrxPlan* plan = new TrxPlan();
    plan->command = stmt->command;
    return plan;
}

Plan* Optimizer::create_show_plan_tree(const ShowStatement *stmt){
    ShowPlan* plan = new ShowPlan();
    plan->type = stmt->type;
    plan->schema = stmt->schema;
    plan->name = stmt->name;
    plan->next = NULL;
    return plan;
}

}

