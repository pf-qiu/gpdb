#include "gpss_server_utils.h"

#include <random>
#include <memory.h>

const char hextable[] = "0123456789abcdef";

RandomID::RandomID()
{
    std::random_device rd;
    std::mt19937 e(rd());

    data[0] = e();
    data[1] = e();
    data[2] = e();
    data[3] = e();
}
RandomID::RandomID(const RandomID &id)
{
    memcpy(data, id.data, sizeof(data));
}
std::string RandomID::String()
{
    std::string id;
    int s = sizeof(data);
    unsigned char *str = (unsigned char *)data;
    id.reserve(s * 2 + 1);
    for (int i = 0; i < s; i++)
    {
        unsigned char c = str[i];
        id.push_back(hextable[c >> 4]);
        id.push_back(hextable[c & 0xF]);
    }

    return id;
}