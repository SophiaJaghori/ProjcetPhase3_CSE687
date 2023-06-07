#pragma once
#include <string>
// Interface Class

class ICReducer {
public:
    //virtual ~ICReducer() { ; }
    virtual double Add(double, double) = 0;
    virtual void clear() = 0;
  //  virtual void do_accumulate_job(std::string lines)=0;
    virtual void Reduce(std::map<std::string, std::string> wordAccumCount)=0;
    virtual std::map<std::string,int> exportData()=0;
    virtual void bufferFlush()=0;
    virtual std::string getReducerFileName()=0;
};

