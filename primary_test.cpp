#include <bits/stdc++.h>
using namespace std;
#define TEST

#ifndef TEST
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>
#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>
#include <sys/mman.h>
#endif

#define THREAD_NUM 4
#define MAX_EDGE_NUM        280000                      //转账记录不超过2W8
#define MAX_OUT_EDGE_NUM    50                          //各结点出度最多50
#define CHAR_RES_SIZE3      500000*18                   //3个数字,每个不超过5位数  2个‘,’ 1个‘\n’ 最多占18bit
#define CHAR_RES_SIZE4      500000*24                   //4个数字,每个不超过5位数  3个‘,’ 1个‘\n’ 最多占24bit
#define CHAR_RES_SIZE5      1000000*30
#define CHAR_RES_SIZE6      2000000*36
#define CHAR_RES_SIZE7      3000000*42

int  inDegrees [MAX_EDGE_NUM];                   //各结点的入度个数
int  outDegrees[MAX_EDGE_NUM];                   //各结点的出度个数
int  topSort[MAX_EDGE_NUM];                      //各结点拓扑排序后的入度个数
char strIDs [MAX_EDGE_NUM][10];                  //id映射表(char型)
int  inputs [MAX_EDGE_NUM*2];                    //输入容器
int  Graph  [MAX_EDGE_NUM][MAX_OUT_EDGE_NUM];    //邻接表建图
int  inGraph[MAX_EDGE_NUM][MAX_OUT_EDGE_NUM];    //入边的邻接表建图
int  tmp[MAX_EDGE_NUM];
int  ids[MAX_EDGE_NUM];                          //id映射表(int型)

char thread1_res1[CHAR_RES_SIZE3];      //存储线程1的3环的结果    8.5Mb
char thread1_res2[CHAR_RES_SIZE4];      //存储线程1的4环的结果   11.4Mb
char thread1_res3[CHAR_RES_SIZE5];      //存储线程1的5环的结果   28.6Mb
char thread1_res4[CHAR_RES_SIZE6];      //存储线程1的6环的结果   68.6Mb
char thread1_res5[CHAR_RES_SIZE7];      //存储线程1的7环的结果    120Mb

char thread2_res1[CHAR_RES_SIZE3];
char thread2_res2[CHAR_RES_SIZE4];
char thread2_res3[CHAR_RES_SIZE5];
char thread2_res4[CHAR_RES_SIZE6];
char thread2_res5[CHAR_RES_SIZE7];

char thread3_res1[CHAR_RES_SIZE3];
char thread3_res2[CHAR_RES_SIZE4];
char thread3_res3[CHAR_RES_SIZE5];
char thread3_res4[CHAR_RES_SIZE6];
char thread3_res5[CHAR_RES_SIZE7];

char thread4_res1[CHAR_RES_SIZE3];
char thread4_res2[CHAR_RES_SIZE4];
char thread4_res3[CHAR_RES_SIZE5];
char thread4_res4[CHAR_RES_SIZE6];
char thread4_res5[CHAR_RES_SIZE7];

char* thread1_res[] = {thread1_res1,thread1_res2,thread1_res3,thread1_res4,thread1_res5};
char* thread2_res[] = {thread2_res1,thread2_res2,thread2_res3,thread2_res4,thread2_res5};
char* thread3_res[] = {thread3_res1,thread3_res2,thread3_res3,thread3_res4,thread3_res5};
char* thread4_res[] = {thread4_res1,thread4_res2,thread4_res3,thread4_res4,thread4_res5};

char** Res[] = {thread1_res,thread2_res,thread3_res,thread4_res};

bool vis_th1  [MAX_EDGE_NUM];              //4个线程的标记空间，标记结点是否访问过，是否处于3领域之中
bool vis_th2  [MAX_EDGE_NUM];
bool vis_th3  [MAX_EDGE_NUM];
bool vis_th4  [MAX_EDGE_NUM];
int  range_th1[MAX_EDGE_NUM];
int  range_th2[MAX_EDGE_NUM];
int  range_th3[MAX_EDGE_NUM];
int  range_th4[MAX_EDGE_NUM];
bool* Vis  [4] = {vis_th1,vis_th2,vis_th3,vis_th4};
int*  Range[4] = {range_th1,range_th2,range_th3,range_th4};

int resIndex[THREAD_NUM][5];              //记录每个线程不同环结果的下标
int resCnt  [THREAD_NUM];                 //记录每个线程计算的结果个数
int edgeCnt = 0;                          //转账记录
int nodeCnt = 0;                          //顶点
int maxID   = 0;

//线程参数结构体
typedef struct arg{
    int begin;
    int end;
    int threadID;
}Arg;

#ifdef TEST
inline void readTestData(const string &testFile){
#ifdef TEST
    double s=clock();
#endif
    FILE* stream=fopen(testFile.c_str(),"r");
    int UID,CID,money;
    int i=0;
    while(fscanf(stream,"%u,%u,%u",&UID,&CID,&money)!=EOF){
        inputs[i++]=UID;
        inputs[i++]=CID;
        edgeCnt++;
        maxID = max(max(UID,CID),maxID);
    }
    fclose(stream);
#ifdef TEST
    printf("read:%0.3fs\n",(clock()-s)/CLOCKS_PER_SEC);
    printf("records nums:%d\n",edgeCnt);
#endif
}
#else
void mmapRead(const string& dirPath){
#ifdef TEST
    double s = clock();
#endif
    char *data = nullptr;
    int fp = open(dirPath.c_str(),O_RDONLY);
    struct stat sb;
    fstat(fp, &sb);
    data = (char *)mmap(nullptr,sb.st_size,PROT_READ,MAP_PRIVATE,fp,0);
    int index=0;
    int UID = 0,CID = 0,money = 0;
    for(int i = 0; data[i] != '\n' &&i < sb.st_size; ){
        //get UID
        for(; data[i] != ',' && i < sb.st_size; i++){
            UID = UID*10 + (data[i]-'0');
        }
        i++;
        //get CID
        for(; data[i] != ',' && i < sb.st_size; i++){
            CID = CID*10 + (data[i] - '0');
        }
        i++;
        //move to \n
        for(; data[i] != '\n' && i < sb.st_size;i++){}
        i++;
        inputs[index++] = UID;
        inputs[index++] = CID;
        edgeCnt++;
        maxID = max(max(UID,CID),maxID);
    }
    close(fp);
    munmap(data,sb.st_size);
#ifdef TEST
    printf("read:%0.3fs\n",(clock()-s)/CLOCKS_PER_SEC);
    printf("records nums:%d\n",edgeCnt);
#endif
}
#endif

inline void constructGraph(){
    //计数排序+id映射到0~nodeCnt
    int idSize = maxID+1,nodeNum = edgeCnt*2;
    for(int i=0; i < nodeNum;i++){
        tmp[inputs[i]] = 1;
    }
    for(int i = 1; i < idSize;i++){
        tmp[i] += tmp[i-1];
    }
    nodeCnt = tmp[maxID];
    for(int i = nodeNum; i > 0; i--){
        ids[tmp[inputs[i-1]]-1] = inputs[i-1];
    }

    //直接将排好序的int型id映射为char*
    stack<int> que;
    int id,idLen = 0;
    for(int i = 0;i < nodeCnt;i++){
        char* strID = strIDs[i];        //一个id的储存地址
        id = ids[i];
        if(id == 0) que.push(0);
        while(id != 0){
            que.push(id%10);
            id /= 10;
        }
        while(!que.empty()){
            strID[idLen++] = char(que.top()+'0');
            que.pop();
        }
        idLen = 0;
    }
    //构建图
    int len=edgeCnt*2;
    for(int i=1; i<len; i+=2){
        int UID=ids[inputs[i-1]];
        int CID=ids[inputs[i]];
        Graph[UID][outDegrees[UID]++] = CID;
        inGraph[CID][inDegrees[CID]++] = UID;
    }
#ifdef TEST
     printf("node nums:%d\n",nodeCnt);
#endif
}

//拓扑排序剪枝
inline void preprocess(){
    memcpy(topSort,inDegrees,sizeof(int)*MAX_EDGE_NUM);
    stack<int> stack;
    for(int v=0; v<nodeCnt; ++v){                   //入度为0的结点入栈
        if(!inDegrees[v])
            stack.push(v);
    }
    while(!stack.empty()){
        int& n=stack.top();
        stack.pop();
        for(int i=0; i<outDegrees[n]; ++i){         //遍历n的所有出度结点
            int& v=Graph[n][i];
            if(!--topSort[v])                       //所有出度结点的入度-1，并继续判断，为0则入栈
                stack.push(v);
        }
    }
    //每个结点的入度和出度按从小到大排序
    for(int i=0; i<nodeCnt; ++i){
        sort(Graph[i],Graph[i]+outDegrees[i]);
        sort(inGraph[i],inGraph[i]+inDegrees[i]);
    }
}

//插入一行环
inline void pushRes(int thID, int* path, int depth){
    resCnt[thID]++;
    int n = depth - 3;                    //n环
    char* res = Res[thID][n];             //获得第thID线程的n环结果容器
    int& index = resIndex[thID][n];       //第thID线程的n环结果的下标
    int p = 0;
    for(int i = 0; i<=depth-1; ++i){
        int& val=path[i];                 //取数
        while(strIDs[val][p] != '\0')     //映射
            res[index++] = strIDs[val][p++];
        p = 0;
        res[index++] = ',';
    }
    res[index-1] = '\n';
}

//获取大于head最小的结点的下标(遍历入度）
inline int minIndexIn(int cur,int head){
    int sz=inDegrees[cur];
    if(!sz) return 0;
    if(inGraph[cur][sz-1] < head) return sz;
    for(int i = 0 ;i < sz ; ++i){
        if(head <= inGraph[cur][i])
            return i;
    }
    return sz;
}

//获取大于head最小的结点的下标(遍历出度）
inline int minIndexOut(int cur,int head){
    int sz=outDegrees[cur];
    if(!sz) return 0;
    if (Graph[cur][sz - 1] < head) return sz;
    for (int i = 0 ; i < sz ; ++i) {
        if (head <= Graph[cur][i])
            return i;
    }
    return sz;
}

//深度递归
inline void dfs(int head,int cur,int depth,int* path,int thID,
        unordered_map<int, vector<int>>* memPath2,unordered_set<int>* memPath3){
    bool* vis  = Vis[thID];
    int* range = Range[thID];
    path[depth-1] = cur, vis[cur] = true;
    int i = minIndexOut(cur,head);
    for (; i < outDegrees[cur]; ++i){
        int& v1 = Graph[cur][i];
        if (vis[v1] || range[v1] != head) continue;
        if (memPath2->find(v1) != memPath2->end()){     //子节点v1再走2步就能到达head
            path[depth]=v1, vis[v1] = true;
            for (int& v2 : (*memPath2)[v1]){
                if (vis[v2]) continue;
                path[depth+1]=v2;
                pushRes(thID, path, depth+2);
            }
            vis[v1] = false;
        }
        //目前深度还不到4，或者到4了但其子节点v1再走3步刚好到达head，则继续递归下去
        if (depth < 4 || (depth == 4 && memPath3->find(v1) != memPath3->end()))
            dfs(head,v1,depth+1,path,thID,memPath2,memPath3);
    }
    vis[cur] = false;
}

//3领域出度剪枝
inline void ThreeRangeCut(int* range,int head) {
    int i = minIndexOut(head,head);
    for(; i < outDegrees[head]; ++i){
        int& v1=Graph[head][i];
        range[v1]=head;
        int j = minIndexOut(v1,head);
        for(; j < outDegrees[v1]; ++j){
            int& v2=Graph[v1][j];
            range[v2]=head;
            int k = minIndexOut(v2,head);
            for(;k<outDegrees[v2]; ++k){
                int& v3=Graph[v2][k];
                range[v3]=head;
            }
        }
    }
}

//3领域入度剪枝+提前记忆2和3步
inline void preMemory(int* range,int head,
        unordered_map<int, vector<int>>* memPath2,unordered_set<int>* memPath3){
    int i=minIndexIn(head,head);
    for(; i<inDegrees[head]; ++i){
        int& v1=inGraph[head][i];
        range[v1]=head;
        int j = minIndexIn(v1,head);
        for(; j<inDegrees[v1]; ++j){
            int& v2=inGraph[v1][j];
            range[v2]=head;
            if(memPath2->find(v2) != memPath2->end())
                (*memPath2)[v2].push_back(v1);
            else
                memPath2->emplace(v2, vector<int>{v1});
            int k = minIndexIn(v2,head);
            for(;k<inDegrees[v2];++k){
                int& v3=inGraph[v2][k];
                memPath3->emplace(v3);
                range[v3]=head;
            }
        }
    }
}

inline void* threadFunc(void* arg){
#ifdef TEST
    double s=clock();
#endif
    int& begin=static_cast<Arg*>(arg)->begin;
    int& end  =static_cast<Arg*>(arg)->end;
    int& thID =static_cast<Arg*>(arg)->threadID;
    auto memPath2=new unordered_map<int, vector<int>> (nodeCnt);
    auto memPath3=new unordered_set<int> (nodeCnt);
    int path[7]={0};
    for(int i=begin;i<=end;++i){
        if(!topSort[i] || !outDegrees[i] || !inDegrees[i]) continue;    //排除入度出度和拓扑排序后入度为0得结点
        ThreeRangeCut(Range[thID],i);
        preMemory(Range[thID],i,memPath2,memPath3);
        dfs(i, i, 1, path,thID,memPath2,memPath3);
        memPath2->clear();
        memPath3->clear();
    }
#ifdef TEST
    printf("thread_%d cost:%.3fs\n",thID,(clock()-s)/CLOCKS_PER_SEC);
#endif
    return nullptr;
}

inline void writeRes(const string &outputPath){
#ifdef TEST
    double s=clock();
#endif
    FILE *stream=fopen(outputPath.c_str(), "wb");
    //环的个数
    int sz = 0;
    for(int i = 0; i < THREAD_NUM;i++) {
        sz += resCnt[i];
    }
#ifdef TEST
    printf("loops:%d\n",sz);
#endif
    char strSz[10];
    sprintf(strSz,"%d\n",sz);
    fwrite(strSz, strlen(strSz)*sizeof(char),1,stream);

    //输出各个线程结果，依次输出3、4、5、6、7环
    for(int i = 0; i < 5; ++i) {
        for (int j = 0; j < THREAD_NUM; ++j)
            fwrite(Res[j][i], resIndex[j][i] * sizeof(char), 1, stream);
    }
    fclose(stream);
#ifdef TEST
    printf("fwrite:%.3fs\n",(clock()-s)/CLOCKS_PER_SEC);
#endif
}

//开启线程
inline void createThread(){
    //配置负载
    int slice_arr[] = {nodeCnt/20,nodeCnt/10,nodeCnt/5,nodeCnt-1};
    Arg **arg = (Arg**)malloc(sizeof(Arg*)*THREAD_NUM);
    for(int i = 0; i < THREAD_NUM; ++i){
        arg[i] = (Arg *)malloc(sizeof(Arg));
        arg[i]->threadID = i;
        arg[i]->begin = i==0 ? 0 : slice_arr[i-1]+1;
        arg[i]->end = slice_arr[i];
    }
    pthread_t thread[THREAD_NUM];
    for(int i = 0; i < THREAD_NUM; ++i){
        pthread_create(&thread[i], nullptr,threadFunc,(void*)arg[i]);
    }
    for(int i = 0; i < THREAD_NUM; ++i){
        pthread_join(thread[i], nullptr);
    }
}

inline void solve(const string& readPath,const string& resultPath){
#ifdef TEST
    readTestData(readPath);
#else
    mmapRead(readPath,inputs);
#endif
    constructGraph();
    preprocess();
    createThread();
    writeRes(resultPath);
}

int main(){
#ifdef TEST
    double s=clock();
    //string readPath = "../data/std/test_data.txt";
    string readPath = "../data/2896262/test_data.txt";
    string resultPath = "../result.txt";
#else
    string testFilePath = "/data/test_data.txt";
    string resultPath = "/projects/student/result.txt";
#endif
    solve(readPath,resultPath);
#ifdef TEST
    printf("Total:%.3fs\n",(clock()-s)/CLOCKS_PER_SEC);
#endif
    return 0;
}
