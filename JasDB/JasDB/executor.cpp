//
//  executor.cpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#include<iostream>
#include "executor.hpp"
#include "metadata.hpp"
#include "optimizer.hpp"
using namespace hsql;

namespace jasdb{

void Executor::init() {
    op_tree = generateOperator(plan_tree);
}

bool Executor::exec(){
    return op_tree->exec();
}

BaseOperator* Executor::generateOperator(Plan *plan){
    BaseOperator* op = NULL;
    BaseOperator* next = NULL;
    if(plan->next != NULL){
        next = generateOperator(plan->next);
    }
    switch (plan->plan_type) {
        case kCreate:
            op = new CreateOperator(plan,next);
            break;
        case kDrop:
            op = new DropOperator(plan,next);
            break;
        case kInsert:
            op = new InsertOperator(plan,next);
            break;
        case kUpdate:
            op = new UpdateOperator(plan,next);
            break;
        case kDelete:
            op = new DeleteOperator(plan,next);
            break;
        case kSelect:
            op = new SelectOperator(plan,next);
            break;
        case kScan:{
            ScanPlan* scan_plan = static_cast<ScanPlan*>(plan);
            if(scan_plan->type == kSeqScan){
                op = new SeqScanOperator(plan,next);
            }
            break;
        }
        case kFilter:
            op = new FilterOperator(plan,next);
            break;
        case kTrx:
            op = new TrxOperator(plan,next);
            break;
        case kShow:
            op = new ShowOperator(plan,next);
            break;
        default:
            std::cout << "Not support plan type." << std::endl;
            break;
    }
    return op;
}

bool CreateOperator::exec(TupleIter** iter){
    CreatePlan* plan = static_cast<CreatePlan*>(plan_);
    if(plan->type == kCreateTable){
        // create table
        Table* table = new Table(plan->schema,plan->tableName,plan->columns);
        if(global_meta_data.insert_table(table)){
            //
            if(plan->ifNotExist){
                std::cout<<"Table already existed"<<std::endl;
                return false;
            }
            else{
                std::cout<<"Table already existed"<<std::endl;
                return true;
            }
            delete table;
        }
        std::cout << "Create table successfully" << std::endl;
        return false;
    }
    else if(plan->type == kCreateIndex){
        // create index
        Table* table = global_meta_data.get_table(plan->schema, plan->tableName);
        if(table == NULL){
            std::cout<<"Table not found"<<std::endl;
            return true;
        }
        Index* index = table->get_index(plan->indexName);
        if(index != NULL){
            if(plan->ifNotExist){
                return false;
            }
            else{
                std::cout<<"Index already existsed."<<std::endl;
                return true;
            }
        }
        index = new Index();
        index->name = plan->indexName;
        index->columns = *plan->indexColumns;
        table->add_index(index);
        std::cout << "Create index successfully." << std::endl;
    }
    else{
        std::cout << "Invalid create statement." << std::endl;
        return true;
    }
    return false;
}

bool DropOperator::exec(TupleIter** iter){
    DropPlan* plan = static_cast<DropPlan*>(plan_);
    if(plan->type == kDropSchema){
        if(global_meta_data.drop_schema(plan->schema)){
            if(plan->ifExists){
                std::cout<<"Schema not existsed."<<std::endl;
                return false;
            }
            else{
                std::cout<<"Schema not existsed."<<std::endl;
                return true;
            }
        }
        std::cout<<"Drop schema successfully."<<std::endl;
        return false;
    }
    else if(plan->type == kDropTable){
        if(global_meta_data.drop_table(plan->schema, plan->name)){
            if(plan->ifExists){
                std::cout<<"Table not existsed."<<std::endl;
                return false;
            }
            else{
                std::cout<<"Table not existsed."<<std::endl;
                return true;
            }
        }
        std::cout<<"Drop table successfully."<<std::endl;
        return false;
    }
    else if(plan->type == kDropIndex){
        if(global_meta_data.drop_index(plan->schema, plan->name, plan->indexName)){
            if(plan->ifExists){
                std::cout<<"Index not existsed."<<std::endl;
                return false;
            }
            else{
                std::cout<<"Index not existsed."<<std::endl;
                return true;
            }
        }
        std::cout<<"Drop index successfully."<<std::endl;
        return false;
    }
    else{
        std::cout<<"Invalid drop statement."<<std::endl;
        return true;
    }
    return false;
}

bool InsertOperator::exec(TupleIter** iter){
    InsertPlan* plan = static_cast<InsertPlan*>(plan_);
    TableStore* table_store = plan->table->getTableStore();
    if(table_store->insertTuple(plan->values)){
        return true;
    }
    std::cout<<"Insert tuple sucessfully."<<std::endl;
    return false;
}

bool UpdateOperator::exec(TupleIter** iter){
    UpdatePlan* update = static_cast<UpdatePlan*>(plan_);
    Table* table = update->table;
    TableStore* table_store = table->getTableStore();
    int upd_cnt = 0;
    while(true){
        TupleIter* tup_iter = NULL;
        if(next->exec(&tup_iter)){
            return true;
        }
        if(tup_iter == NULL){
            break;
        }
        else{
            table_store->updateTuple(tup_iter->tup,tup_iter->idxs,update->values);
            upd_cnt++;
        }
    }
    std::cout<<"Update sucessfully."<<std::endl;
    return false;
}

bool DeleteOperator::exec(TupleIter** iter){
    Table* table = static_cast<DeletePlan*>(plan_)->table;
    TableStore* table_store = table->getTableStore();
    int del_cnt = 0;
    while(true){
        TupleIter* tup_iter = NULL;
        if(next->exec(&tup_iter)){
            return true;
        }
        if(tup_iter == NULL){
            break;
        }
        else{
            table_store->deleteTuple(tup_iter->tup);
            del_cnt++;
        }
    }
    std::cout<<"Delete sucessfully."<<std::endl;
    return false;
}

bool TrxOperator::exec(TupleIter** iter){
    TrxPlan* plan = static_cast<TrxPlan*>(plan_);
    switch (plan->command) {
        case hsql::kBeginTransaction:
            global_transaction.begin();
            std::cout << "Start transaction" << std::endl;
            break;
        case hsql::kCommitTransaction:
            global_transaction.commit();
            std::cout << "Commit transaction" << std::endl;
            break;
        case hsql::kRollbackTransaction:
            global_transaction.rollback();
            std::cout << "Rollback transaction" << std::endl;
            break;
        default:
            break;
    }
    return false;
}

bool ShowOperator::exec(TupleIter** iter){
    ShowPlan* show_plan = static_cast<ShowPlan*>(plan_);
    if(show_plan->type == kShowTables){
        std::vector<Table*> tables;
        global_meta_data.get_all_tables(&tables);
        std::cout << "Tables:" << std::endl;
        for(auto table : tables){
            std::cout << TableNameToString(table->schema, table->name) << std::endl;
        }
    }
    else if(show_plan->type == kShowColumns){
        Table* table = global_meta_data.get_table(show_plan->schema, show_plan->name);
        if(table == NULL){
            std::cout << "Table not found" << std::endl;
            return true;
        }
        std::cout << "Columns in" << TableNameToString(table->schema, table->name) << std::endl;
        for(auto col_def : *table->columns){
            if(col_def->type.data_type == DataType::CHAR || col_def->type.data_type == DataType::VARCHAR){
                std::cout << col_def->name << "\t"
                            << DataTypeToString(col_def->type.data_type) << "("
                            << col_def->type.length << ")" << std::endl;
            }
            else{
                std::cout << col_def->name << "\t"
                            << DataTypeToString(col_def->type.data_type) << std::endl;
            }
        }
    }
    else{
        std::cout << "Invalid 'Show' statement." << std::endl;
        return true;
    }

    return false;
}

bool SelectOperator::exec(TupleIter** iter){
    SelectPlan* plan = static_cast<SelectPlan*>(plan_);
    std::vector<std::vector<Expr*>> tuples;
    while(true){
        TupleIter* tup_iter = NULL;
        if(next->exec(&tup_iter)){
            return true;
        }
        if(tup_iter == NULL){
            break;
        }
        else{
            tuples.push_back(tup_iter->values);
        }
    }
    PrintTuple(plan->out_cols,plan->col_ids,tuples);
    return false;
}

bool SeqScanOperator::exec(TupleIter** iter){
    ScanPlan* plan = static_cast<ScanPlan*>(plan_);
    TableStore* table_store = plan->table->getTableStore();
    Tuple* tup = NULL;
    if(finish){
        return false;
    }
    if(next_tuple == NULL){
        tup = table_store->seqScan(NULL);
    }
    else{
        tup = next_tuple;
    }
    if(tup == NULL){
        *iter = NULL;
        return false;
    }
    TupleIter* tup_iter = new TupleIter(tup);
    table_store->parseTuple(tup,tup_iter->values);
    tuples.push_back(tup_iter);
    *iter = tup_iter;
    next_tuple = table_store->seqScan(tup);
    if(next_tuple == NULL){
        // no next
        finish = true;
    }
    return false;
}

bool FilterOperator::exec(TupleIter** iter){
    *iter = NULL;
    while(true){
        TupleIter* tup_iter = NULL;
        if(next->exec(&tup_iter)){
            return true;
        }
        if(tup_iter == NULL){
            break;
        }
        if(execEqualExpr(tup_iter)){
            *iter = tup_iter;
            break;
        }
    }
    return false;
}

bool FilterOperator::execEqualExpr(TupleIter *iter){
    FilterPlan* filter = static_cast<FilterPlan*>(plan_);
    Expr* val = filter->val;
    auto col_id = filter->idx;
    Expr* col_val = iter->values[col_id];
    if(col_val->type != val->type){
        return false;
    }
    if(col_val->type == kExprLiteralInt){
        return (col_val->ival == val->ival);
    }
    if(col_val->type == kExprLiteralString){
        return (strcmp(col_val->name, val->name) == 0);
    }
    return false;
}


}
