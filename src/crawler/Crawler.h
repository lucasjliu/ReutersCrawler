//
//  Crawler.h
//
//  by jiahuan.liu
//  10/29/2016
//

#ifndef _CRAWLER_H
#define _CRAWLER_H

#include <set>
#include <vector>
#include <mutex>
#include <thread>
#include <stdio.h>

#include "Socket.h"
#include "Http.h"
#include "Exception.h"
#include "Common.h"
#include "Logger.h"
#include "ThreadPool.h"
#include "Util.h"

class MyCrawler
{
public:
    struct Article
    {
        std::string sTitle;
        std::string sBody;
        
        std::string genContent() const
        {
            return sTitle + "|" + sBody;
        }
    };
public:
    MyCrawler()
    {
        reset();
    }
    ~MyCrawler() {stop(); fclose(_docsRecordFile);}
    void init(std::string sHostUrl,
              int iArticleTrdNum = 5,
              int iArchiveTrdNum = 2,
              int iListTrdNum = 2,
              int iMaxDoc = 100,
              std::string sWorkPlace = "./",
              std::set<std::string> targetArchive =
              {
                  "/news/archive/worldNews",
                  "/news/archive/businessNews",
                  "/news/archive/technologyNews"
              })
    {
        stop();
        reset();
        
        _hostUrl.parse(sHostUrl);
        _archiveTasks.push(_hostUrl);
        _archiveSem.notify();
        
        _iArticleTrdNum = iArticleTrdNum;
        _iArchiveTrdNum = iArchiveTrdNum;
        _iListTrdNum = iListTrdNum;
        _iMaxDocNum = iMaxDoc;
        _sWorkPlace = sWorkPlace;
        
        _targetArchives = targetArchive;
    }
    void run();
    void stop() {}
    void reset() {}
    void crawlArticle();
    void crawlList();
    void crawlArchive();
    bool fetch(Url& url, std::string& sContent);
    void parseArticle(const std::string& sContent, Article& stArticle);
    void parseList(const std::string& sContent, std::vector<Url>& vUrls);
    void parseArchive(const std::string& sContent, std::vector<Url>& vUrls, std::vector<Url>& vArchives);
    void log(std::string sMsg);
    void addArchive(Url url);
private:
    static std::string _genDocName(const int& docid)
    {
        char sBuf[10] = {0};
        sprintf(sBuf, "%09d", docid);
        return std::string(sBuf);
    }
    static std::string _genPageUrl(const Url& archiveUrl, const int& pageNum)
    {
        return archiveUrl.getHost() + archiveUrl.getResourcePath() +
            "?view=page&page=" + Common::tostr(pageNum) + "&pageSize=10";
    }
    int _doRequest(Url& url, HttpResponse& stRsp, int iTimeout = 3000);
    int _doRequest(const std::string& sUrl, HttpResponse& stRsp, int iTimeout = 3000);
    bool _isEnd() {return _docidSvr.get() >= _iMaxDocNum;}
protected:
    Deque<Url>      _articleTasks;
    Semaphore       _articleSem;
    Deque<Url>      _archiveTasks;
    Semaphore       _archiveSem;
    Deque<Url>      _listTasks;
    Semaphore       _listSem;
    
    IdServer        _docidSvr;
    Md5Server       _archiveMd5;
    Md5Server       _articleMd5;
    
    std::mutex      _mtxLog;
    
    int             _iArticleTrdNum;
    int             _iArchiveTrdNum;
    int             _iListTrdNum;
    int             _iMaxDocNum;
    
    Url             _hostUrl;
    
    std::string     _sWorkPlace;
    
    std::vector<std::thread> _trdList;
    
    std::set<std::string> _targetArchives;
    
    FILE*           _docsRecordFile;
};

#endif//_CRAWLER_H
