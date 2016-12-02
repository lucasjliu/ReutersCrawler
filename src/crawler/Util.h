//
//  Util.h
//
//  by jiahuan.liu
//  10/31/2016
//

#ifndef _UTIL_H
#define _UTIL_H

#include <mutex>
#include <set>
#include <deque>
#include <fstream>
#include <sstream>

#include "Common.h"
#include "Logger.h"
#include "Exception.h"

class Atomic
{
public:
    typedef int value_type;
    Atomic(const value_type& at = 0) :_value(at) {}
    void reset(const value_type& at = 0) {_value = at;}
    value_type inc()
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return ++_value;
    }
    value_type dec()
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return --_value;
    }
    value_type get() const
    {
        return _value;
    }
private:
    std::mutex _mutex;
    value_type _value;
};

class IdServer
{
public:
    typedef int value_type;
    IdServer(const value_type& start = 0) :_id(start) {}
    void init(const value_type& start) {_id.reset(start);}
    value_type genId() const
    {
        value_type ret = _id.get();
        _id.inc();
        return ret;
    }
    value_type get() {return _id.get();}
private:
    mutable Atomic _id;
};

class Md5Server
{
public:
    bool find(const std::string& md5) const
    {
        std::lock_guard<std::mutex> lock(_mtx);
        
        return _set.find(md5) != _set.end();
    }
    bool add(const std::string& md5)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        
        if (_set.find(md5) != _set.end())
        {
            return false;
        }
        
        _set.insert(md5);
        
        return true;
    }
    bool del(const std::string& md5)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        
        if (_set.find(md5) == _set.end())
        {
            return false;
        }
        
        _set.erase(md5);
        
        return true;
    }
private:
    mutable std::mutex      _mtx;
    std::set<std::string>   _set;
};


template<typename T>
class Deque
{
public:
    void push(const T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _dq.push_back(t);
    }
    bool pop_back(T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_dq.empty())
            return false;
        t = _dq.back();
        _dq.pop_back();
        return true;
    }
    void push_front(const T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        _dq.push_front(t);
    }
    bool pop(T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_dq.empty())
            return false;
        t = _dq.front();
        _dq.pop_front();
        return true;
    }
    bool get(T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_dq.empty())
            return false;
        t = _dq.back();
        return true;
    }
    bool front(T& t)
    {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_dq.empty())
            return false;
        t = _dq.front();
        return true;
    }
private:
    std::mutex    _mtx;
    std::deque<T> _dq;
};

class File
{
public:
    enum
    {
        NEW = 1,
        APP
    };
    static bool write(std::string filepath, const std::string& sContent, int eMode)
    {
        std::ios_base::openmode mode;
        if (eMode == NEW)
        {
            std::fstream test(filepath);
            if (test.is_open())
            {
                test.close();
                return false;
            }
            mode = std::ios::out;
        }
        else if (eMode == APP)
        {
            mode = std::ios::out|std::ios::app;
        }
        else
        {
            mode = std::ios::out;
        }
        
        std::fstream fs(filepath, mode);
        if (!fs.is_open())
        {
            LOG << "fail to save file:" + filepath << END;
            return false;
        }
        fs << sContent;
        fs.close();
        
        return true;
    }
private:
    //std::fstream _fs;
};

class HtmlUtil
{
public:
    struct Exception : ::Exception
    {
        Exception(const std::string& strerr) : ::Exception(strerr) {}
        Exception(const std::string& strerr, int ierr)
        : ::Exception(strerr, ierr) {}
        ~Exception() noexcept {}
    };
public:
    //getLine
    typedef std::string::size_type Pos;
    static Pos skipTo(const std::string& content, Pos& pos, std::string tag)
    {
        if (pos == std::string::npos) return pos;
        return (pos = content.find(tag, pos));
    }
    static Pos skip(const std::string& content, Pos& pos, std::string tag)
    {
        if (pos == std::string::npos) return pos;
        pos = content.find(tag, pos);
        if (pos != std::string::npos)
            pos += tag.length();
        return pos;
    }
    static std::string getBetween(const std::string& content, Pos& pos,
                                  const std::string& begTag, const std::string& endTag)
    {
        if (pos == std::string::npos) return "";
        skip(content, pos, begTag);
        std::string sRet;
        if (pos != std::string::npos)
        {
            Pos pos1 = content.find(endTag, pos);
            if (pos1 != std::string::npos)
            {
                sRet = content.substr(pos, pos1 - pos);
                pos1 += endTag.length();
            }
            pos = pos1;
        }
        return sRet;
    }
    static std::string getBetween(const std::string& content, const Pos& p,
                                  const std::string& begTag, const std::string& endTag)
    {
        Pos pos = p;
        return getBetween(content, pos, begTag, endTag);
    }
    static void filter(std::string& sUrl)
    {
        std::stringstream ss(sUrl);
        std::string sTmp;
        sUrl.clear();
        while (ss >> sTmp)
        {
            sUrl += sTmp;
        }
    }
    static void preClean(std::string& content, std::set<std::string> tags)
    {
        
    }
};

#endif
