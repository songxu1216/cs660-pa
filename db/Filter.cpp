#include <db/Filter.h>
#include <iostream>
using namespace db;

Filter::Filter(Predicate p, DbIterator *child) {
    // TODO pa3.1: some code goes here
    this->child = child;
    *this->p = p;
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return p;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return child->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    child->open();
    Operator::open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    child->close();
    Operator::close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    close();
    open();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    return {child};
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if(!children.empty()){
        child = children[0];
    }
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    while(child->hasNext()){
        auto tuple = child->next();
        if (p->filter(tuple)) {
            return tuple;
        }
    }
    return std::nullopt;
}
