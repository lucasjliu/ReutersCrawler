//
//  Crawler.cpp
//
//  by jiahuan.liu
//  10/29/2016
//
#include <unistd.h>

#include "Crawler.h"

void MyCrawler::run()
{
    _docsRecordFile = fopen((_sWorkPlace + "docs.txt").c_str(), "w+");
    
    for (int i = 0; i < _iArticleTrdNum; i++)
    {
        _trdList.push_back(std::thread(&MyCrawler::crawlArticle, this));
    }
    for (int i = 0; i < _iArchiveTrdNum; i++)
    {
        _trdList.push_back(std::thread(&MyCrawler::crawlArchive, this));
    }
    for (int i = 0; i < _iListTrdNum; i++)
    {
        _trdList.push_back(std::thread(&MyCrawler::crawlList, this));
    }
    for (int i = 0; i < (int)_trdList.size(); ++i)
    {
        _trdList[i].join();
    }
}

void MyCrawler::crawlArticle()
{
    while (true)
    {
        _articleSem.wait();
        
        if (_isEnd())
            return;
        
        Url url;
        if(!_articleTasks.pop(url))
        {
            continue;
        }
        
        std::string sContent;
        fetch(url, sContent);
        if (sContent.empty())
        {
            log("[fetch]exception: no content: " + url.genUrl());
            continue;
        }
        
        Article stArticle;
        parseArticle(sContent, stArticle);
        
        int docid = _docidSvr.genId();
        
        std::string docname = _genDocName(docid);
        
        std::vector<std::string> vTitle = std::move(Common::sepstr<std::string>(stArticle.sTitle, " ", false));
        std::vector<std::string> vBody = std::move(Common::sepstr<std::string>(stArticle.sBody, " ", false));
        
        FLOG_DATA(_docsRecordFile) << docid << "|" << url.genUrl() << "|" << stArticle.sTitle << "|" <<
            docname << "|" << vTitle.size() << "|" << vBody.size();
        
        File::write(_sWorkPlace + "docs/" + docname,
                    Common::tostr(docid) + "|" + stArticle.genContent(),
                    File::NEW);
    }
}

void MyCrawler::parseArticle(const std::string& sContent, Article& stArticle)
{
    std::string::size_type pos = 0, end;
    
    if (HtmlUtil::skipTo(sContent, pos, "article-headline") == std::string::npos)
    {
        pos = 0;
        HtmlUtil::skipTo(sContent, pos, "<title>");
    }
    if (pos == std::string::npos)
    {
        pos = 0;
        log("[parseArticle]exception: headline not found");
    }
    else
    {
        stArticle.sTitle = HtmlUtil::getBetween(sContent, pos, ">", "<");
        Common::trim(stArticle.sTitle);
        //HtmlUtil::filter(stArticle.sTitle);
    }
    
    std::string sLine;
    end = sContent.find("<header class=\"module-header next-article-header\">");
    while (HtmlUtil::skipTo(sContent, pos, "<p>") != std::string::npos)
    {
        if (pos >= end)
            break;
        sLine = HtmlUtil::getBetween(sContent, pos, "<p>", "</p");
        //HtmlUtil::filter(sLine);
        std::string::size_type tagBeg = 0, tagEnd;
        while (HtmlUtil::skipTo(sLine, tagBeg, "<") != std::string::npos)
        {
            tagEnd = tagBeg;
            HtmlUtil::skip(sLine, tagEnd, ">");
            HtmlUtil::skip(sLine, tagEnd, ">");
            if (tagEnd == std::string::npos)
            {
                log("[parseArticle]exception processing html: " + sLine);
                sLine.clear();
                break;
            }
            sLine.erase(tagBeg, tagEnd - tagBeg);
        }
        if (!sLine.empty())
        {
            Common::trim(sLine);
            stArticle.sBody += sLine + " ";
        }
    }
}

void MyCrawler::crawlList()
{
    while (true)
    {
        _listSem.wait();
        
        if (_isEnd())
            return;
        
        Url url;
        if(!_listTasks.pop(url))
        {
            continue;
        }
        
        std::string sContent;
        if (!fetch(url, sContent))
        {
            _listTasks.push(url);
            continue;
        }
        
        //TODO: log//////////////////////////////////////////
        log("[crawlList]:" + url.genUrl());
        
        std::vector<Url> vUrls;
        parseList(sContent, vUrls);
        
        for (Url stUrl : vUrls)
        {
            _articleTasks.push(stUrl);
            _articleSem.notify();
        }
    }
}

void MyCrawler::parseList(const std::string& sContent, std::vector<Url>& vUrls)
{
    std::string::size_type pos = 0;
    std::string sUrl;
    
    while(HtmlUtil::skipTo(sContent, pos, "class=\"story-title\"") != std::string::npos)
    {
        sUrl = HtmlUtil::getBetween(sContent, pos, "<a href=\"", "\"");
        HtmlUtil::filter(sUrl);
        if (sUrl.find("/article/") == std::string::npos)
            continue;
        if (!_articleMd5.add(sUrl))
            continue;
        vUrls.push_back(_hostUrl.buildByRelatPath(sUrl));
    }
}

void MyCrawler::crawlArchive()
{
    std::vector<Url> vUrls, vArchives;
    while(true)
    {
        _archiveSem.wait();
        
        if (_isEnd())
            return;
        
        Url url;
        if(!_archiveTasks.pop(url))
        {
            continue;
        }
        
        std::string sContent;
        if (!fetch(url, sContent))
        {
            continue;
        }
        
        vUrls.clear();
        vArchives.clear();
        
        parseArchive(sContent, vUrls, vArchives);
        
        for (auto url : vUrls)
        {
            //log("[parseArchive]url found: " + url.genUrl());
            _archiveTasks.push(url);
            _archiveSem.notify();
        }
        for (auto url : vArchives)
        {
            log("[parseArchive]archive found: " + url.genUrl());
            addArchive(url);
        }
    }
}

void MyCrawler::parseArchive(const std::string& sContent,
                             std::vector<Url>& vUrls,
                             std::vector<Url>& vArchives)
{
    Url url;
    std::string sPath;
    std::string::size_type pos = 0;
    
    while (HtmlUtil::skip(sContent, pos, "<a href=") != std::string::npos)
    {
        std::string sPath = HtmlUtil::getBetween(sContent, pos, "\"", "\"");
        HtmlUtil::filter(sPath);
        Url tmpUrl;
        
        if (sPath.empty())
        {
            continue;
        }
        int templength = (int)sPath.length() - 4;
        if(sPath.rfind(".jpg") == templength||
           sPath.rfind(".gif") == templength||
           sPath.rfind(".mp3") == templength||
           sPath.rfind(".wma") == templength||
           sPath.rfind(".pdf") == templength||
           sPath.rfind(".doc") == templength||
           sPath.rfind(".swf") == templength||
           sPath.rfind(".exe") == templength||
           sPath.rfind(".rar") == templength||
           sPath.rfind(".zip") == templength||
           sPath.rfind(".ico") == templength||
           sPath.rfind(".png") == templength||
           sPath.rfind(".css") == templength)
        {
            continue;
        }
        
        if (sPath[0] == '/')
        {
            tmpUrl = _hostUrl.buildByRelatPath(sPath);
        }
        else
        {
            tmpUrl.parse(sPath);
        }
        
        sPath = tmpUrl.getResourcePath();
        
        if (tmpUrl.getHost() != _hostUrl.getHost() ||
            sPath.find("/article/") != std::string::npos ||
            sPath.find("/video/") != std::string::npos)
            
        {
            continue;
        }
        
        if (!_archiveMd5.add(sPath))
        {
            continue;
        }
        
        if (sPath.find("/archive/") != std::string::npos)
        {
            vArchives.push_back(tmpUrl);
        }
        else
        {
            vUrls.push_back(tmpUrl);
        }
    }
}

bool MyCrawler::fetch(Url& url, std::string& sContent)
{
    HttpResponse rsp;
    int iRet = _doRequest(url, rsp);
    if (iRet < 0 || rsp.getStatus() != 200)
    {
        //log("[fetch]fail: "+Common::tostr(iRet)+" "+url.genUrl());
        return false;
    }
    sContent = rsp.getContent();
    return true;
}

void MyCrawler::addArchive(Url url)
{
    if (!_targetArchives.empty())
    {
        if (_targetArchives.find(url.getResourcePath()) == _targetArchives.end())
        {
            return;
        }
    }
    
    int beg = 1, end = beg, leap = 1000;
    HttpResponse rsp;
    
    std::string pageUrl = _genPageUrl(url, beg);
    int iRet = _doRequest(url, rsp);
    if (iRet < 0 || rsp.getStatus() != 200)
    {
        return;
    }
    
    while (true)
    {
        if (leap == 0) break;
        end += leap;
        
        pageUrl = _genPageUrl(url, end);
        iRet = _doRequest(pageUrl, rsp);
        if (iRet < 0)
        {
            end -= leap;
            continue;
        }
        if (rsp.getStatus() != 200)
        {
            end -= leap;
            leap /= 2;
        }
    }
    
    for (int page = end; page >= beg; page--)
    {
        
        _listTasks.push(Url(_genPageUrl(url, page)));
        
        _listSem.notify();
    }
}

void MyCrawler::log(std::string sMsg)
{
    std::lock_guard<std::mutex> lock(_mtxLog);
    LOG << Common::now2str("[%H:%M:%S]") << sMsg << END;
}

int MyCrawler::_doRequest(Url& url, HttpResponse& stRsp, int iTimeout)
{
    std::string sUrl = url.genUrl();
    return _doRequest(sUrl, stRsp, iTimeout);
}

int MyCrawler::_doRequest(const std::string& sUrl, HttpResponse& stRsp, int iTimeout)
{
    HttpRequest req;
    req.createGetRequest(sUrl, true);
    HttpHeader* header = req.getHeader();
    req.setUserAgent("Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.8.1.14)");
    header->setAccept("text/xml,application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5");
    header->setAcceptLanguage("en");
    header->setItem("Accept-Charset", "utf-8;q=0.7,*;q=0.7");
    //header->setItem("Keep-Alive", "3000");
    //header->setItem("Connection", "keep-alive");
    
    std::string sSend = req.encode();
    
    TcpClient tcp;
    Url url(sUrl);
    tcp.init(url.getHost(), Common::strto<uint16_t>(url.getPort()), iTimeout);
    
    int iRet = tcp.send(sSend.c_str(), sSend.length());
    if (iRet != ClientSocket::EM_SUCCESS)
    {
        return iRet;
    }
    
    stRsp.reset();
    std::string sRecv;
    size_t iRecv = 0, iBuf = 10240, iCur;
    char* sBuf = new char[iBuf];
    
    while (true)
    {
        iCur = iBuf;
        memset(sBuf, 0, iBuf);
        iRet = tcp.recv(sBuf, iCur);
        if (iRet == ClientSocket::EM_CLOSE)
        {
            break;
        }
        else if (iRet != ClientSocket::EM_SUCCESS)
        {
            return iRet;
        }
        iRecv += iCur;
        sRecv.append(sBuf, iCur);
    }
    
    //////////////////////////////////////
//    if (sRecv.back() == '0')
//    {
//        sRecv.pop_back();
//        Common::trimright(sRecv, " ");
//    }
    //////////////////////////////////////
    
    __TRY__
    iRet = stRsp.decode(sRecv.c_str(), sRecv.length());
    __CATCH__(Exception &e)
    log("[doRequest]exception: invalid page or redirection: " + sUrl);
    __END__
    
    delete []sBuf;
    
    return iRet == true ? 0 : -9;
}