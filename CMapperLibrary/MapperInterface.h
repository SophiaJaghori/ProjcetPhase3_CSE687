#pragma once
#include <string>
// Interface Class
class ICMapper {
public:
    //virtual ~ICMapper() { ; }
    virtual double Add(double, double)=0;
    virtual std::string Map(std::string filename, std::string strline)=0;
    virtual void Map(std::string filename, std::string strline, std::string partition) = 0;
    virtual  std::multimap<std::string, int, std::less<std::string>> exportData()=0;
    virtual void bufferFlush() = 0;
};

