#include "index_base.h"

itemid_t::~itemid_t()
{
}

bool itemid_t::operator==(const itemid_t &other) const {
    return row == other.row;
}

bool itemid_t::operator!=(const itemid_t &other) const {
    return !(*this == other);
}

void itemid_t::operator=(const itemid_t &other){
    this->row = other.row;
}

void itemid_t::init() {
    valid = false;
    row = NULL;
    next = NULL;
}
