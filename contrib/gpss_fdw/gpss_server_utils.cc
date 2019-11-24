#include "gpss_server_utils.h"

#include <random>

bool GenerateID(unsigned char* buffer, int size)
{
    std::random_device rd;
    std::mt19937 e(rd());
    
    int w = std::mt19937::word_size / 8;

    int n = size / w;
    
    auto buf1 = reinterpret_cast<std::mt19937::result_type*>(buffer);
    for (int i = 0; i < n; i++)
    {
        buf1[i] = e();
    }

    buffer += n * w;
    int remain = size % w;
    std::mt19937::result_type r = e();
    for (int i = 0; i < remain; i++)
    {
        buffer[i] = r & 0xFF;
        r >>= 8;
    }
    return true;
}