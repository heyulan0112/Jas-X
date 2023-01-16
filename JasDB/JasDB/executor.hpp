//
//  executor.hpp
//  JasDB
//
//  Created by jasmine on 1/13/23.
//

#ifndef executor_hpp
#define executor_hpp

#include <stdio.h>
#include "optimizer.hpp"

namespace jasdb{

class TupleIter{
public:
    Tuple* tuple;
    std::vector<Expr*> values;
    TupleIter(Tuple* t) : tuple(t){}
    ~TupleIter(){
        for(auto expr : values){
            delete expr;
        }
    }
};

class BaseOperator{
public:
    Plan* plan_;
    BaseOperator* next;
    BaseOperator(Plan* p, BaseOperator* n) : plan_(p), next(n){}
    virtual ~BaseOperator(){
        delete next;
    }
    virtual bool exec(TupleIter** iter = NULL) = 0;
};

class CreateOperator : public BaseOperator{
public:
    CreateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~CreateOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class DropOperator : public BaseOperator {
public:
    DropOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~DropOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class InsertOperator : public BaseOperator{
public:
    InsertOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~InsertOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class UpdateOperator : public BaseOperator{
public:
    UpdateOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~UpdateOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class DeleteOperator : public BaseOperator{
public:
    DeleteOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~DeleteOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class TrxOperator : public BaseOperator{
public:
    TrxOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~TrxOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class ShowOperator : public BaseOperator{
public:
    ShowOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~ShowOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class SelectOperator : public BaseOperator{
public:
    SelectOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~SelectOperator(){}
    bool exec(TupleIter** iter = NULL) override;
};

class SeqScanOperator : public BaseOperator{
public:
    std::vector<TupleIter*> tuples;
    SeqScanOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){
        finish = false;
        next_tuple = NULL;
    }
    ~SeqScanOperator(){
        for(auto iter : tuples){
            delete iter;
        }
    }
    bool exec(TupleIter** iter = NULL) override;
    bool finish;
    Tuple* next_tuple;
};

class FilterOperator : public BaseOperator{
public:
    FilterOperator(Plan* plan, BaseOperator* next) : BaseOperator(plan,next){}
    ~FilterOperator(){}
    bool exec(TupleIter** iter = NULL) override;
    bool execEqualExpr(TupleIter* iter);
};

class Executor{
public:
    BaseOperator* generateOperator(Plan* plan);
    Plan* plan_tree;
    BaseOperator* op_tree;
    Executor(Plan* plan) : plan_tree(plan){}
    ~Executor(){}
    void init();
    bool exec();
};

}

#endif /* executor_hpp */
