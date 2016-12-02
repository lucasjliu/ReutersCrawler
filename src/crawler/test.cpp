//
//  test_crawler.cpp
//
//  by jiahuan.liu
//  11/01/2016
//

#include <set>

#include "Common.h"
#include "Crawler.h"

int main(int argc,char *argv[])
{
    LOG << "begin" << END;
    if (argc <= 6)
    {
        LOG << "too few parameters." << END;
        return -1;
    }
    std::string sHostUrl = argv[1];
    int iArticleTrdNum = Common::strto<int>(argv[2]);
    int iArchiveTrdNum = Common::strto<int>(argv[3]);
    int iListTrdNum = Common::strto<int>(argv[4]);
    int iMaxDoc = Common::strto<int>(argv[5]);
    std::string sWorkPlace = argv[6];
    std::set<std::string> targetArchive;
    for (int i = 7; i < argc; i++)
    {
        targetArchive.insert(argv[i]);
    }
    __TRY__
    MyCrawler crawler;
    crawler.init(sHostUrl, iArticleTrdNum, iArchiveTrdNum, iListTrdNum, iMaxDoc, sWorkPlace, targetArchive);
    crawler.run();
    __CATCH__(Exception& e)
    LOG << e.what() << END;
    __END__
    LOG << "end" << END;
    return 0;
}