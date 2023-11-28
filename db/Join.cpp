#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) {
    // TODO pa3.1: some code goes here
    this->p =p;
    this->child1 = child1;
    this->child2 = child2;
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return p;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return child1->getTupleDesc().getFieldName(p->getField1());
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return child2->getTupleDesc().getFieldName(p->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return TupleDesc::merge(child1->getTupleDesc(), child2->getTupleDesc());
}

void Join::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    child1->open();
    child2->open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    child1->close();
    child2->close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    close();
    open();
}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    return {child1, child2};
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    child1 = children[0];
    child2 = children[1];
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    while (true) {
        if (!outerTuple) {
            if (!child1->hasNext()) {
                return std::nullopt; // No more tuples
            }
            outerTuple = child1->next();
            child2->rewind(); // Reset inner relation for new outer tuple
        }

        while (child2->hasNext()) {
            innerTuple = child2->next();
            if (p->filter(&*outerTuple, &*innerTuple)) {
                // Concatenate the tuples and return the result
                TupleDesc newTd = TupleDesc::merge(outerTuple->getTupleDesc(), innerTuple->getTupleDesc());
                Tuple mergedTuple(newTd);
                int totalFields = outerTuple->getTupleDesc().numFields() + innerTuple->getTupleDesc().numFields();
                for (int i = 0; i < totalFields; ++i) {
                    if (i < outerTuple->getTupleDesc().numFields()) {
                        const Field &field = outerTuple->getField(i);
                        mergedTuple.setField(i, &field);
                    } else {
                        const Field &field = innerTuple->getField(i - outerTuple->getTupleDesc().numFields());
                        mergedTuple.setField(i, &field);
                    }
                }
                return mergedTuple;
            }
        }

        // Exhausted inner relation, move to next tuple in outer relation
        outerTuple = std::nullopt;
        }
}
