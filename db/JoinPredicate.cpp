#include <db/JoinPredicate.h>
#include <db/Tuple.h>

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2):field1(field1), op(op), field2(field2) {
    // TODO pa3.1: some code goes here

}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
    // TODO pa3.1: some code goes here
    return t1->getField(field1).compare(op, &t2->getField(field2));


}

int JoinPredicate::getField1() const {
    // TODO pa3.1: some code goes here
    return field1;
}

int JoinPredicate::getField2() const {
    // TODO pa3.1: some code goes here
    return field2;
}

Predicate::Op JoinPredicate::getOperator() const {
    // TODO pa3.1: some code goes here
    return op;
}
