#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) {
    // TODO pa3.1: some code goes here
    *this->p = p;
    this->child1 = child1;
    this->child2 = child2;
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return this->p;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

std::string HashEquiJoin::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return child1->getTupleDesc().getFieldName(p->getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return child2->getTupleDesc().getFieldName(p->getField1());
}

void HashEquiJoin::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child1->open();
    child2->open();
}

void HashEquiJoin::close() {
    // TODO pa3.1: some code goes here
    child1->close();
    child2->close();
    Operator::close();
}

void HashEquiJoin::rewind() {
    // TODO pa3.1: some code goes here
    close();
    open();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    // TODO pa3.1: some code goes here
    return {child1, child2};
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if(!children.empty()){
        child1 =  children[0];
        child2 =  children[1];
    }
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    // TODO pa3.1: some code goes here

}
