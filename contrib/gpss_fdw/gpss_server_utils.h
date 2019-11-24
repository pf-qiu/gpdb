#pragma once

#include <string>

class RandomID
{
public:
    RandomID();
    RandomID(const RandomID &id);
    std::string String();

private:
    unsigned int data[4];
};