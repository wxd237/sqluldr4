/*
   NAME
     dataUldr.c - Using v8 OCI function to rewrite loufangxin's unload script.

   MODIFIED   (MM/DD/YY) 
     Baoqiu Yang    2008.05.31 -  Initial rewrite.
     Baoqiu Yang    2008.06.21 -  finish internal LOB type support.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>

#include <oratypes.h>
#include <oci.h>
#include <ocidfn.h>
#include <ocidem.h>

/* Constants used in this program. */
#define MAX_ITEM_BUFFER_SIZE    33
#define MAX_SELECT_LIST_SIZE    1024
#define MAXBUFLEN               5000
#define ROW_BATCH_SIZE          500000

#if defined(_WIN32)
#define STRNCASECMP memicmp
#else
#define STRNCASECMP strncasecmp
#endif

//#define  MIN(a,b) ((a) > (b) ? (b) : (a))

struct COLUMN
{
  /* Describe */
  //text            colname[MAX_ITEM_BUFFER_SIZE];
  text            *colname;
  ub4             colname_len;
  ub4             colwidth;
  ub2             coltype;
  ub4             buflen;
  ub2             precision;
  ub2             scale;

  /*+ Fetch */
  OCIDefine       *dfnhp;
  OCILobLocator   *blob; //blob locator
  OCILobLocator   *clob; //clob locator
  ub1             *colbuf;    //output variable
  sb2             *indp;
  ub2             *col_retlen;
  ub2             *col_retcode;

  /*+ Point to next column */
  struct          COLUMN *next;
};

/*Defined functions*/
void  checkerr(OCIError *errhp,sword status);
void  initialize ();
void  logon (char *v_user,char *v_pass,char *v_host);
void  logout();
void  cleanup();
void  freeColumn(struct COLUMN *col);
void  printRow(text *fname,OCISvcCtx *svchp,OCIStmt *stmhp,struct COLUMN *col,text *field, int flen,text *record, int rlen, int batch, int header);
void  printRowInfo(ub4 row);
void  destr(char *src,char *v_user,char *v_pass,char *v_host);
void stream_read_clob(OCILobLocator *lobl, FILE *fp);
void stream_read_blob(OCILobLocator *lobl, FILE *fp);
sword preparSql(OCIStmt *stmhp, text *sql_statement);
sword executeSql(OCISvcCtx *svchp,OCIStmt *stmhp,ub4 execount);
sword getColumns(FILE *fpctl,OCIStmt *stmhp, struct COLUMN *collist);
int   convertOption(const ub1 *src, ub1* dst, int mlen);
ub1   getHexIndex(char c);
FILE  *openFile(const text *fname, text tempbuf[], int batch);

static void describe_table (FILE *fpctl,text *tabname);
static void describe_column(FILE *fpctl,OCIParam *parmp, ub4 parmcnt);

/*global env variables*/
static OCIEnv       *envhp  = (OCIEnv *)0;
static OCIServer    *srvhp  = (OCIServer *)0;
static OCIError     *errhp  = (OCIError *)0;
static OCISvcCtx    *svchp  = (OCISvcCtx *)0;
static OCIStmt      *stmhp  = (OCIStmt *)0;
static OCIDescribe  *dschp  = (OCIDescribe *)0;
static OCISession   *sesshp = (OCISession *)0;

ub4     DEFAULT_ARRAY_SIZE = 50;
ub4     DEFAULT_LONG_SIZE = 32768;
int     return_code = 0;
FILE    *fp_log = NULL;

int main(int argc, char *argv[])
{
  sword n,i,argcount=0;
  int   v_help=0;
  
  struct COLUMN col;

  text tempbuf[1024];
  text user[132]="";
  text query[32768]="";
  text sqlfname[255]="";
  text tabname[132]="";
  text tabmode[132]="INSERT";
  text fname[255]="uldrdata.txt";
  text ctlfname[256]="";
  text field[132]=",";
  text logfile[256]="";
  text record[132]="\n";
  
  int  flen,rlen;
  int  buffer= 16777216;
  int  hsize = 0;
  int  ssize = 0;
  int  bsize = 0;
  int  serial= 0;
  int  trace = 0;
  int  batch = 0;
  int  header= 0;

  
  char *p_user=malloc(50);
  char *p_pass=malloc(50);
  char *p_host=malloc(20);
  
  FILE *fp;
  FILE *fpctl;
  
  flen = rlen = 1;
     
  for(i=0;i<argc;i++)
  {
    if (STRNCASECMP("user=",argv[i],5)==0)
    {
        memset(user,0,132);
        memcpy(user,argv[i]+5,MIN(strlen(argv[i]) - 5,131));
    }
    else if (STRNCASECMP("query=",argv[i],6)==0)
    {
        memset(query,0,8192);
        memcpy(query,argv[i]+6,MIN(strlen(argv[i]) - 6,8191));
    }
    else if (STRNCASECMP("sql=",argv[i],4)==0)
    {
        memset(sqlfname,0,132);
        memcpy(sqlfname,argv[i]+4,MIN(strlen(argv[i]) - 4,254));
    }
    else if (STRNCASECMP("file=",argv[i],5)==0)
    {
        memset(fname,0,132);
        memcpy(fname,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
    }     
    else if (STRNCASECMP("field=",argv[i],6)==0)
    {
        memset(field,0,132);
        flen=convertOption(argv[i]+6,field,MIN(strlen(argv[i]) - 6,131));
    }
    else if (STRNCASECMP("record=",argv[i],7)==0)
    {
        memset(record,0,132);
        rlen=convertOption(argv[i]+7,record,MIN(strlen(argv[i]) - 7,131));
    }     
    else if (STRNCASECMP("log=",argv[i],4)==0)
    {
        memset(logfile,0,256);
        memcpy(logfile,argv[i]+4,MIN(strlen(argv[i]) - 4,254));
    }     
    else if (STRNCASECMP("table=",argv[i],6)==0)
    {
        memset(tabname,0,132);
        memcpy(tabname,argv[i]+6,MIN(strlen(argv[i]) - 6,128));
    }     
    else if (STRNCASECMP("mode=",argv[i],5)==0)
    {
        memset(tabmode,0,132);
        memcpy(tabmode,argv[i]+5,MIN(strlen(argv[i]) - 5,128));
    }     
    else if (STRNCASECMP("head=",argv[i],5)==0)
    {
        memset(tempbuf,0,132);
        memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,128));
        header = 0;
        if (STRNCASECMP(tempbuf,"YES",3) == 0) header = 1;
        if (STRNCASECMP(tempbuf,"ON",3) == 0) header = 1;
    }     
    else if (STRNCASECMP("sort=",argv[i],5)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
        ssize = atoi(tempbuf);
        if (ssize < 0) ssize = 0;
        if (ssize > 512) ssize = 512;
    }     
    else if (STRNCASECMP("buffer=",argv[i],7)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+7,MIN(strlen(argv[i]) - 7,254));
        buffer = atoi(tempbuf);
        if (buffer < 8) buffer = 8;
        if (ssize > 100) buffer = 100;
        buffer = buffer * 1048576;
    }     
    else if (STRNCASECMP("long=",argv[i],5)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
        DEFAULT_LONG_SIZE = atoi(tempbuf);
        if (DEFAULT_LONG_SIZE < 100) DEFAULT_LONG_SIZE = 100;
        if (DEFAULT_LONG_SIZE > 32767) DEFAULT_LONG_SIZE = 32767;
    }
    else if (STRNCASECMP("array=",argv[i],6)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
        DEFAULT_ARRAY_SIZE = atoi(tempbuf);
        if (DEFAULT_ARRAY_SIZE < 5) DEFAULT_ARRAY_SIZE = 5;
        if (DEFAULT_ARRAY_SIZE > 2000) DEFAULT_ARRAY_SIZE = 2000;
    }
    else if (STRNCASECMP("hash=",argv[i],5)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
        hsize = atoi(tempbuf);
        if (hsize < 0) hsize = 0;
        if (hsize > 512) hsize = 512;
    }     
    else if (STRNCASECMP("read=",argv[i],5)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+5,MIN(strlen(argv[i]) - 5,254));
        bsize = atoi(tempbuf);
        if (bsize < 0) bsize = 0;
        if (bsize > 512) bsize = 512;
    }     
    else if (STRNCASECMP("batch=",argv[i],6)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
        batch = atoi(tempbuf);
        if (batch < 0) batch = 0;
        if (batch == 1) batch = 2;
    }     
    else if (STRNCASECMP("serial=",argv[i],7)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+7,MIN(strlen(argv[i]) - 7,254));
        serial = atoi(tempbuf);
    }     
    else if (STRNCASECMP("trace=",argv[i],6)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
        trace = atoi(tempbuf);
    }     
    else if (STRNCASECMP("-help",argv[i],4)==0)
    {
    	  v_help=1;
    }     
  }
  
  if (strlen(sqlfname) > 0)
  {
    fp = fopen(sqlfname,"r+");
    if (fp != NULL)
    {
      while(!feof(fp))
      {
        memset(tempbuf,0,1024);
        fgets(tempbuf,1023,fp);
        strcat(query,tempbuf);
        strcat(query," ");
      }
      fclose(fp);
    }
  }

  if (strlen(user)==0 || strlen(query)==0)
  {
    if (v_help)
    {
       printf("ociuldr: Release 2.1 by hrb_qiuyb\n");
       printf("\n");
       printf("Usage: %s user=... query=... field=... record=... file=...\n",argv[0]);
       printf("Notes:\n");
       printf("       user  = username/password@tnsname\n");
       printf("       sql   = SQL file name\n");
       printf("       query = select statement\n");
       printf("       field = seperator string between fields\n");
       printf("       record= seperator string between records\n");
       printf("       file  = output file name(default: mydata.txt)\n");
       printf("       read  = set DB_FILE_MULTIBLOCK_READ_COUNT at session level\n");
       printf("       sort  = set SORT_AREA_SIZE & SORT_AREA_RETAINED_SIZE at session level (UNIT:MB) \n");
       printf("       hash  = set HASH_AREA_SIZE at session level (UNIT:MB) \n");
       printf("       serial= set _serial_direct_read to TRUE at session level\n");
       printf("       trace = set event 10046 to given level at session level\n");
       printf("       table = table name in the sqlldr control file\n");
       printf("       mode  = sqlldr option, INSERT or APPEND or REPLACE or TRUNCATE \n");
       printf("       log   = log file name, prefix with + to append mode\n");
       printf("       long  = maximum long field size\n");
       printf("       array = array fetch size\n");
       printf("       buffer= sqlldr READSIZE and BINDSIZE, default 16 (MB)\n");
       printf("\n");
       printf("  for field and record, you can use '0x' to specify hex character code,\n");
       printf("  \\r=0x%02x \\n=0x%02x |=0x%0x ,=0x%02x \\t=0x%02x\n",'\r','\n','|',',','\t');
       printf("  for more hex character code,you can use unix command:man ascii\n");
       exit(0);
    }
    else 
    { 	
      printf("Datauldr: Release 2.0\n");
      printf("\n");
  	  printf("Usage: %s user=... query=... field=... record=... file=...\n",argv[0]);
  	  printf("More information please use:%s -help\n",argv[0]);
      printf("\n");
  	  exit(0);    
    }
  }		

  //de username,password,host
  destr(user,p_user,p_pass,p_host);

  initialize();
  logon(p_user,p_pass,p_host);
  
  /*stmt handle*/
  OCIHandleAlloc((dvoid *) envhp, (dvoid **) &stmhp, OCI_HTYPE_STMT,
                 (size_t) 0, (dvoid **) 0);
  
  /*prepary session env*/
  prepareSql(stmhp,"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");
  executeSql(svchp,stmhp,1);
  prepareSql(stmhp,"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SSXFF'");
  executeSql(svchp,stmhp,1);
  prepareSql(stmhp,"ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SSXFF TZH:TZM'");
  executeSql(svchp,stmhp,1);

  if (bsize)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET DB_FILE_MULTIBLOCK_READ_COUNT=%d",bsize);
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
  }
  if (hsize)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET HASH_AREA_SIZE=%d",hsize * 1048576);
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET \"_hash_multiblock_io_count\"=128");
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
  }
  if (serial)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET \"_serial_direct_read\"=TRUE");
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
  }
  if (ssize)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET SORT_AREA_SIZE=%d",ssize * 1048576);
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET SORT_AREA_RETAINED_SIZE=%d",ssize * 1048576);
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET \"_sort_multiblock_read_count\"=128");
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
  }
  if (trace)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET EVENTS='10046 TRACE NAME CONTEXT FOREVER,LEVEL %d'", trace);
    prepareSql(stmhp,tempbuf);
    executeSql(svchp,stmhp,1);
  }
  
  //generate sql*loader controlfile
  if(strlen(tabname))
  {
      memset(ctlfname,0,256);
      sprintf(ctlfname,"%s_sqlldr.ctl",tabname);
      fpctl = fopen(ctlfname,"wb+");
      
      if(fpctl != NULL)
      {
         if (!header)
            fprintf(fpctl,"OPTIONS(BINDSIZE=%d,READSIZE=%d,ERRORS=-1,ROWS=50000)\n", buffer, buffer);
         else
            fprintf(fpctl,"OPTIONS(BINDSIZE=%d,READSIZE=%d,SKIP=1,ERRORS=-1,ROWS=50000)\n", buffer, buffer);
         fprintf(fpctl,"LOAD DATA\n");
         fprintf(fpctl,"INFILE '%s' \"STR X'", fname);
         for(i=0;i<strlen(record);i++) fprintf(fpctl,"%02x",record[i]);
         fprintf(fpctl,"'\"\n");
         fprintf(fpctl,"%s INTO TABLE %s\n", tabmode, tabname);
         fprintf(fpctl,"FIELDS TERMINATED BY X'");
         for(i=0;i<strlen(field);i++) fprintf(fpctl,"%02x",field[i]);
         fprintf(fpctl,"' TRAILING NULLCOLS \n");
         fprintf(fpctl,"(\n");
      }
  }

  /*prepary sql*/
  prepareSql(stmhp,query);
  
  if (executeSql(svchp,stmhp,0))
  	return;
  
  /*get and define columns*/
  getColumns(fpctl,stmhp,&col);
  
  /*output result*/
  printRow(fname,svchp,stmhp,&col,field,flen,record,rlen,batch, header);
  
  /*release resource*/
  freeColumn(&col);
  
  logout();
  cleanup();
}

/* ----------------------------------------------------------------- */
/* initialize environment, allocate handles, etc.                    */
/* ----------------------------------------------------------------- */

void initialize ()
{
  printf ("\nInitializing the environment..\n");

  OCIEnvCreate((OCIEnv **) &envhp,OCI_THREADED|OCI_OBJECT,(dvoid *)0,
        (dvoid * (*)(dvoid *, size_t)) 0,
        (dvoid * (*)(dvoid *, dvoid *, size_t))0,
        (void (*)(dvoid *, dvoid *)) 0,
        (size_t) 0, (dvoid **) 0);

  /* error handle */
  OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR,
                         (size_t) 0, (dvoid **) 0);

  /* server handle */
  OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &srvhp, OCI_HTYPE_SERVER,
                         (size_t) 0, (dvoid **) 0);
  /* svcctx handle*/
  OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX,
                         (size_t) 0, (dvoid **) 0);

  /* set attribute server context in the service context */
  OCIAttrSet ((dvoid *) svchp, OCI_HTYPE_SVCCTX, (dvoid *)srvhp,
                     (ub4) 0, OCI_ATTR_SERVER, (OCIError *) errhp);
}

/* ----------------------------------------------------------------- */
/* attach to the server and log on as SCOTT/TIGER                    */
/* ----------------------------------------------------------------- */

void logon (char *v_user,char *v_pass,char *v_host)
{
  printf ("Logging on as %s..\n", v_user);
  OCIHandleAlloc ((dvoid *) envhp, (dvoid **)&sesshp,
                  (ub4) OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);

  if (strlen(v_host)==0)
  {
   checkerr(errhp,OCIServerAttach (srvhp,errhp,(text *)0,(sb4)0,OCI_DEFAULT));
  }		
  else
  {
  	if (OCIServerAttach (srvhp,errhp,(text *)v_host,(sb4)strlen("v_host"),OCI_DEFAULT)==OCI_SUCCESS)
    {
      printf ("Connect to %s sucessful!\n",v_host);
    }
    else
    {
  	 printf ("Connect to %s failed!\n",v_host);
    }
  } 

  OCIAttrSet ((dvoid *)sesshp, (ub4)OCI_HTYPE_SESSION,
              (dvoid *)v_user, (ub4)strlen((char *)v_user),
              OCI_ATTR_USERNAME, errhp);

  OCIAttrSet ((dvoid *)sesshp, (ub4)OCI_HTYPE_SESSION,
              (dvoid *)v_pass, (ub4)strlen((char *)v_pass),
              OCI_ATTR_PASSWORD, errhp);

  checkerr (errhp, OCISessionBegin (svchp,  errhp, sesshp, OCI_CRED_RDBMS,
                                    (ub4) OCI_DEFAULT));
  printf ("Logged on\n");

  OCIAttrSet ((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX,
              (dvoid *) sesshp, (ub4) 0,
              (ub4) OCI_ATTR_SESSION, errhp);
}

/*-------------------------------------------------------------------*/
/* Logoff and disconnect from the server.                            */
/*-------------------------------------------------------------------*/

void logout()
{
  printf ("\n\nFreeing statement handle..\n");
  OCIHandleFree ((dvoid *) stmhp, (ub4) OCI_HTYPE_STMT);

  printf ("Logging off...\n");
  OCISessionEnd (svchp, errhp, sesshp, (ub4) 0);
}

/*-------------------------------------------------------------------*/
/* Free handles.                                                     */
/*-------------------------------------------------------------------*/

void cleanup()
{
  printf ("\nFreeing handles..\n");
  if (errhp)  OCIServerDetach (srvhp, errhp, (ub4) OCI_DEFAULT );
  if (srvhp)  OCIHandleFree((dvoid *) srvhp, (CONST ub4) OCI_HTYPE_SERVER);
  if (svchp)  OCIHandleFree((dvoid *) svchp, (CONST ub4) OCI_HTYPE_SVCCTX);
  if (errhp)  OCIHandleFree((dvoid *) errhp, (CONST ub4) OCI_HTYPE_ERROR);
  if (sesshp) OCIHandleFree((dvoid *) sesshp,(CONST ub4) OCI_HTYPE_SESSION);
}

/*-------------------------------------------------------------------*/
/* Free column struc.                                                */
/*-------------------------------------------------------------------*/

void freeColumn(struct COLUMN *col)
{
  //boolean is_init;
  struct COLUMN *p,*temp;
  p=col->next;

  col->next = NULL;
  while(p!=NULL)
  {
    free(p->colbuf);
    free(p->indp);
    free(p->col_retlen);
    free(p->col_retcode);
    
    //is_init=FALSE;
    //OCILobLocatorIsInit(envhp,errhp,p->blob,is_init);
    //if (is_init==TRUE)
    	//OCIDescriptorFree(p->blob,OCI_DTYPE_LOB);

    //is_init=FALSE;
    //OCILobLocatorIsInit(envhp,errhp,p->clob,&is_init);
    //if (is_init==TRUE)
    	//OCIDescriptorFree(p->clob,OCI_DTYPE_LOB);
    	
    
    temp=p;
    p=temp->next;
    free(temp);
  }
}

/* ----------------------------------------------------------------- */
/* retrieve error message and print it out.                          */
/* ----------------------------------------------------------------- */

void checkerr(OCIError *errhp,sword status)
{
  text errbuf[512];
  sb4 errcode = 0;

  switch (status)
  {
  case OCI_SUCCESS:
    break;
  case OCI_SUCCESS_WITH_INFO:
    (void) printf("Error - OCI_SUCCESS_WITH_INFO\n");
    break;
  case OCI_NEED_DATA:
    (void) printf("Error - OCI_NEED_DATA\n");
    break;
  case OCI_NO_DATA:
    (void) printf("Error - OCI_NODATA\n");
    break;
  case OCI_ERROR:
    (void) OCIErrorGet ((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                    errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    (void) printf("Error - %.*s\n", 512, errbuf);
    break;
  case OCI_INVALID_HANDLE:
    (void) printf("Error - OCI_INVALID_HANDLE\n");
    break;
  case OCI_STILL_EXECUTING:
    (void) printf("Error - OCI_STILL_EXECUTE\n");
    break;
  case OCI_CONTINUE:
    (void) printf("Error - OCI_CONTINUE\n");
    break;
  default:
    break;
  }
}

/* ----------------------------------------------------------------- */
/* generate sql*loader control & define columns                      */
/* ----------------------------------------------------------------- */

sword getColumns(FILE *fpctl,OCIStmt *stmhp, struct COLUMN *collist)
{
  OCIParam      *paramhp;
  ub4           col;
  ub4           numcols;       //select-list columns
  struct COLUMN *tempcol;
  struct COLUMN *nextcol;

  nextcol = collist;

  //get table describ info
  printf("\n");

  //get columns
  checkerr(errhp,OCIAttrGet(stmhp, OCI_HTYPE_STMT, &numcols,0, OCI_ATTR_PARAM_COUNT, errhp));

  /* Describe the select-list items. */
  for (col = 0; col < numcols; col++)
  {
      tempcol = (struct COLUMN *) malloc(sizeof(struct COLUMN));
      tempcol-> indp        = (sb2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(sb2));
      tempcol-> col_retlen  = (ub2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(ub2));
      tempcol-> col_retcode = (ub2 *)malloc(DEFAULT_ARRAY_SIZE * sizeof(ub2));
      tempcol-> colname     =malloc(MAX_ITEM_BUFFER_SIZE);
      memset(tempcol-> colname,0,MAX_ITEM_BUFFER_SIZE);

      tempcol->next = NULL;
      tempcol->colbuf = NULL;

      tempcol->buflen = MAX_ITEM_BUFFER_SIZE;

      /* get parameter for column col*/
      checkerr(errhp, OCIParamGet(stmhp, OCI_HTYPE_STMT, errhp, (void **)&paramhp, col+1));
      /* get data-type of column col */
      checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&tempcol->coltype, 0, OCI_ATTR_DATA_TYPE, errhp));
      checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&tempcol->colname, &tempcol->colname_len, (ub4)OCI_ATTR_NAME, errhp));
      checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&tempcol->colwidth, 0, OCI_ATTR_DATA_SIZE, errhp));
      checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&tempcol->precision, 0, OCI_ATTR_PRECISION, errhp));
      checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&tempcol->scale, 0, OCI_ATTR_SCALE,errhp));
      
      //tempcol->colname[tempcol->colname_len]='\0';
      //printf("colname:%s\n",tempcol->colname);
      
      nextcol->next = tempcol;
      nextcol=tempcol;
      
      switch(nextcol->coltype)
      {
          case SQLT_DATE:
          case SQLT_DAT:
              if(fpctl != NULL)
                 fprintf(fpctl,"  %s DATE \"YYYY-MM-DD HH24:MI:SS\"", nextcol->colname);
              break;
          case SQLT_TIMESTAMP: /* TIMESTAMP */
              if(fpctl != NULL)
                 fprintf(fpctl,"  %s TIMESTAMP \"YYYY-MM-DD HH24:MI:SSXFF\"", nextcol->colname);
              break;
          case SQLT_TIMESTAMP_TZ: /* TIMESTAMP WITH TIMEZONE */
              if(fpctl != NULL)
                 fprintf(fpctl,"  %s TIMESTAMP WITH TIME ZONE \"YYYY-MM-DD HH24:MI:SSXFF TZH:TZM\"", nextcol->colname );
              break;
          case SQLT_LBI:  /* LONG RAW */
             if(fpctl != NULL)
                fprintf(fpctl,"  %s CHAR(%d) ", nextcol->colname, 2 * DEFAULT_LONG_SIZE);
             break;
          case SQLT_BLOB: /* BLOB */
          	 DEFAULT_ARRAY_SIZE = 1;
          	 OCIDescriptorAlloc((dvoid *) envhp, (dvoid **) &nextcol->blob,
          	                    (ub4)OCI_DTYPE_LOB, (size_t) 0, (dvoid **) 0);
             if(fpctl != NULL)
             {	
                fprintf(fpctl,"  lobfile_col%d  FILLER CHAR,\n",col+1);
                fprintf(fpctl,"  %s LOBFILE(lobfile_col%d) TERMINATED BY EOF NULLIF lobfile_col%d = 'NONE' ",
                                 nextcol->colname, col+1,col+1);
             }   
          	 break;
          case SQLT_CLOB: /* BLOB */
          	 DEFAULT_ARRAY_SIZE = 1;
          	 OCIDescriptorAlloc((dvoid *) envhp, (dvoid **) &nextcol->clob,
          	                    (ub4)OCI_DTYPE_LOB, (size_t) 0, (dvoid **) 0);
             if(fpctl != NULL)
             {	
                fprintf(fpctl,"  lobfile_col%d  FILLER CHAR,\n",col+1);
                fprintf(fpctl,"  %s LOBFILE(lobfile_col%d) TERMINATED BY EOF NULLIF lobfile_col%d = 'NONE' ",
                                 nextcol->colname, col+1,col+1);
             }   
          	 break;
          case SQLT_RDD:
              if(fpctl != NULL)
                fprintf(fpctl, "  %s CHAR", nextcol->colname);
              break;
          case SQLT_INT:
          case SQLT_NUM:
             if(fpctl != NULL)
                fprintf(fpctl,"  %s CHAR", nextcol->colname);
             break;
          case SQLT_FILE: /* BFILE */
             if(fpctl != NULL)
                fprintf(fpctl,"  %s CHAR(%d)", nextcol->colname,DEFAULT_LONG_SIZE);
             break;
          default:
             if(fpctl != NULL)
                fprintf(fpctl,"  %s CHAR", nextcol->colname);
             break;
      }
      if (col<numcols-1)
      {
      	if (fpctl != NULL)
    	    fprintf(fpctl,",\n");
      }

      if (nextcol->colwidth > DEFAULT_LONG_SIZE || nextcol->colwidth == 0)
          nextcol->colwidth = DEFAULT_LONG_SIZE;
      /* add one more byte to store the ternimal char of string */
      nextcol->colwidth=nextcol->colwidth+1;

      //nextcol->colname[MAX_ITEM_BUFFER_SIZE]='\0';
      nextcol->colbuf = malloc((int)(DEFAULT_ARRAY_SIZE * nextcol->colwidth));
      memset(nextcol->colbuf,0,(int)(DEFAULT_ARRAY_SIZE * nextcol->colwidth));


      //define output
      if (nextcol->coltype==SQLT_BLOB)
      {	
        checkerr(errhp,OCIDefineByPos(stmhp, &nextcol->dfnhp, errhp, col+1, 
                       (dvoid *) &nextcol->blob,(sb4) -1, (ub2)SQLT_BLOB,nextcol->indp,
                       (ub2 *)nextcol->col_retlen,(ub2 *)nextcol->col_retcode, OCI_DEFAULT));
      }
      else if (nextcol->coltype==SQLT_CLOB)             
      {
        checkerr(errhp,OCIDefineByPos(stmhp, &nextcol->dfnhp, errhp, col+1, 
                       (dvoid *) &nextcol->clob,(sb4) -1, (ub2)SQLT_CLOB, nextcol->indp,
                       (ub2 *)nextcol->col_retlen,(ub2 *)nextcol->col_retcode, OCI_DEFAULT));
      }
      else
      {
        checkerr(errhp,OCIDefineByPos(stmhp, &nextcol->dfnhp, errhp, col+1, 
                       (dvoid *) nextcol->colbuf,nextcol->colwidth, SQLT_STR, nextcol->indp,
                       (ub2 *)nextcol->col_retlen,(ub2 *)nextcol->col_retcode, OCI_DEFAULT));
      }		
    		
  }
  if(fpctl != NULL)
  {	
    fprintf(fpctl,"\n");
    fprintf(fpctl,")\n");
  }  
}

/* ----------------------------------------------------------------- */
/* output select result to files                                     */
/* ----------------------------------------------------------------- */

void printRow(text *fname,OCISvcCtx *svchp,OCIStmt *stmhp,struct COLUMN *col,text *field, int flen,text *record, int rlen, int batch, int header)
{
  ub4           colcount;
  ub4           tmp_rows;
  ub4           tmp_rows_size;
  ub4           rows;
  ub4           j;
  sword         rc;
  sword         r;
  ub4           c;
  ub4           trows;
  struct COLUMN *p;
  struct COLUMN *cols[1024];
  text tempbuf[512];
  FILE *fp;
  FILE *fp_lob = NULL;   //
  text lob_filename[30]; //
  
  int bcount=1;
  trows=0;
  colcount=0;
  
  p = col->next;
  while(p != NULL)
  {
    cols[colcount] = p;
    p=p->next;
    colcount=colcount+1;
  }
  
  memset(tempbuf,0,512);
  memset(lob_filename,0,30);

  if((fp = openFile(fname,tempbuf,bcount)) == NULL) 
  {
    fprintf((fp_log == NULL?stdout:fp_log),"ERROR -- Cannot write to file : %s\n", tempbuf);
    return_code = 6;
    return;
  }


  if (header)
  {
    for(c=0;c<colcount;c++)
    {
       fprintf(fp,"%s",cols[c]->colname);
       if (c < colcount - 1) 
          fwrite(field,flen,1,fp);
    }
    fwrite(record,rlen,1,fp);
  }

  printRowInfo(trows);
  for (;;)
  {
    rows = DEFAULT_ARRAY_SIZE;

    rc=OCIStmtFetch(stmhp, errhp, DEFAULT_ARRAY_SIZE, 0, OCI_DEFAULT);    

    if (rc !=0)
    {
      if (rc!= OCI_NO_DATA)
      {
        return_code = 7;
        checkerr(errhp,rc);
      }	
      
      checkerr(errhp,OCIAttrGet((dvoid *) stmhp, (ub4) OCI_HTYPE_STMT,(dvoid *)&tmp_rows,
                 (ub4 *) &tmp_rows_size, (ub4)OCI_ATTR_ROWS_FETCHED,errhp));
      rows = tmp_rows % DEFAULT_ARRAY_SIZE;
    }		
    
    for(r=0;r<rows;r++)
    {
       for(c=0;c<colcount;c++)
       {
          if (*(cols[c]->indp+r) >= 0)
          {

             if (cols[c]->coltype == 24) //not long type
             {
                /* fprintf(fp, "%010d", 2 * *(cols[c]->col_retlen+r)); */
                /* fwrite(cols[c]->colbuf+(r* cols[c]->dsize),*(cols[c]->col_retlen+r),1,fp); */
                for(j=0;j < *(cols[c]->col_retlen+r); j++)
                {
                   fprintf(fp, "%02x", cols[c]->colbuf[r * cols[c]->colwidth + j]);
                }
             }
             else if (cols[c]->coltype == 113) //blob type
             {
               sprintf((char *) lob_filename, (char *)"LF_%d_%d.blob",c+1,trows+1);
               fwrite((void *)lob_filename,sizeof(lob_filename),1,fp);               
               fp_lob = fopen((char *)lob_filename, (const char *) "wb");
               if ( !(fp_lob))
               {
                 printf("ERROR: Failed to open file(%s).\n",lob_filename);
                 return;
               }
               stream_read_blob(cols[c]->blob, fp_lob);
               fclose(fp_lob);
             }
             else if (cols[c]->coltype == 112) //clob type
             {
               sprintf((char *) lob_filename, (char *)"LF_%d_%d.clob",c+1,trows+1);
               fwrite((void *)lob_filename,sizeof(lob_filename),1,fp);               
               fp_lob = fopen((char *)lob_filename, (const char *) "w");
               if ( !(fp_lob))
               {
                 printf("ERROR: Failed to open file(%s).\n",lob_filename);
                 return;
               }
               stream_read_clob(cols[c]->clob, fp_lob);
               fclose(fp_lob);
             }	
             else
             {
                fwrite(cols[c]->colbuf+(r* cols[c]->colwidth),*(cols[c]->col_retlen+r),1,fp);
             }
          }
          if (c < colcount - 1)
              fwrite(field,flen,1,fp);
       }
       fwrite(record,rlen,1,fp);
       trows=trows+1;
       if (trows % ROW_BATCH_SIZE  == 0)
       {
          printRowInfo(trows);
          if(batch && ((trows / ROW_BATCH_SIZE) % batch) == 0)
          {
             fprintf((fp_log == NULL?stdout:fp_log),"         output file %s closed at %u rows.\n", tempbuf, trows);
             fclose(fp);
             bcount =bcount+1;
             memset(tempbuf,0,512);
             if((fp = openFile(fname,tempbuf,bcount)) == NULL) 
             {
               fprintf((fp_log == NULL?stdout:fp_log),"ERROR -- Cannot write to file : %s\n", tempbuf);
               return_code = 6;
               return;
             }
             if (header)
             {
                for(c=0;c<colcount;c++)
                {
                   fprintf(fp,"%s",cols[c]->colname);
                   if (c < colcount - 1)
                      fwrite(field,flen,1,fp);
                }
                fwrite(record,rlen,1,fp);
             }
             trows = 0;
          }
       }
    }
    if (rows < DEFAULT_ARRAY_SIZE) break;
  }
  if (trows % ROW_BATCH_SIZE != 0)
     printRowInfo(trows);
  fclose(fp);
  fprintf((fp_log == NULL?stdout:fp_log),"         output file %s closed at %u rows.\n\n", tempbuf, trows);
  fflush((fp_log == NULL?stdout:fp_log));
}

sword prepareSql(OCIStmt *stmhp, text *sql_statement)
{
  sword rc;
  rc=OCIStmtPrepare(stmhp, errhp, (text *) sql_statement,
                      (ub4) strlen(sql_statement), OCI_NTV_SYNTAX, OCI_DEFAULT);

  if (rc!=0)
  {
    checkerr(errhp,rc);
    return -1;
  }
  else
    return 0;
}

/* ----------------------------------------------------------------- */
/* execute a sql statement                                           */
/* ----------------------------------------------------------------- */

sword executeSql(OCISvcCtx *svchp,OCIStmt *stmhp,ub4 execount)
{
  sword rc;
  rc=OCIStmtExecute(svchp,stmhp,errhp,execount,0,NULL,NULL,OCI_DEFAULT);

  if (rc!=0)
  {
   checkerr(errhp,rc);
   return -1;
  }
   return 0;
}

/* ----------------------------------------------------------------- */
/* get username,password,alias from use= string                      */
/* ----------------------------------------------------------------- */

void destr(char *src,char *v_user,char *v_pass,char *v_host)
{
  char *r1="/";
  char *r2="@";
  int  n1=0;
  int  n2=0;

  n1=strcspn(src,r1);
  n2=strcspn(src,r2);

  strncpy(v_user,src,n1);
  strncpy(v_pass,&src[n1+1],n2-n1-1);
  strncpy(v_host,&src[n2+1],strlen(src)-n2-1);
}
/* ----------------------------------------------------------------- */
/* open files                                                        */
/* ----------------------------------------------------------------- */

FILE *openFile(const text *fname, text tempbuf[], int batch)
{
   FILE *fp=NULL;
   int i, j, len;
   time_t now = time(0);
   struct tm *ptm = localtime(&now);   
   
   len = strlen(fname);
   j = 0;
   for(i=0;i<len;i++)
   {
      if (*(fname+i) == '%')
      {
          i++;
	        if (i < len)
	        {
            switch(*(fname+i))
            {
              case 'Y':
              case 'y':
                j += sprintf(tempbuf+j, "%04d", ptm->tm_year + 1900);
		            break;
              case 'M':
              case 'm':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mon + 1);
		            break;
              case 'D':
              case 'd':
                j += sprintf(tempbuf+j, "%02d", ptm->tm_mday);
		            break;
              case 'W':
              case 'w':
                j += sprintf(tempbuf+j, "%d", ptm->tm_wday);
		            break;
              case 'B':
              case 'b':
                j += sprintf(tempbuf+j, "%d", batch);
		            break;
              default:
                tempbuf[j++] = *(fname+i);
		            break;
            }
          }
      }
      else
      {
         tempbuf[j++] = *(fname+i);
      }
   }
   tempbuf[j]=0;
   if (tempbuf[0] == '+')
       fp = fopen(tempbuf+1, "ab+");
   else
       fp = fopen(tempbuf, "wb+");
   return fp;
}

/* ----------------------------------------------------------------- */
/* every 500000 row print info to scr or fp_log                      */
/* ----------------------------------------------------------------- */

void  printRowInfo(ub4 row)
{
	time_t now = time(0);
	struct tm *ptm = localtime(&now);
	fprintf((fp_log == NULL?stdout:fp_log),"%8u rows exported at %04d-%02d-%02d %02d:%02d:%02d\n",
                row,
		ptm->tm_year + 1900,
		ptm->tm_mon + 1,
		ptm->tm_mday,
		ptm->tm_hour,
		ptm->tm_min,
		ptm->tm_sec);
        fflush((fp_log == NULL?stdout:fp_log));
}

int convertOption(const ub1 *src, ub1* dst, int mlen)
{
   int i,len,pos;
   ub1 c,c1,c2;

   i=pos=0;
   len = strlen(src);
   
   
   while(i<MIN(mlen,len))
   {
      if ( *(src+i) == '0')
      {
          if (i < len - 1)
          {
             c = *(src+i + 1);
             switch(c)
             {
                 case 'x':
                 case 'X':
                   if (i < len - 3)
                   {
                       c1 = getHexIndex(*(src+i + 2));
                       c2 = getHexIndex(*(src+i + 3));
                       *(dst + pos) = (ub1)((c1 << 4) + c2);
                       i=i+2;
                   }
                   else if (i < len - 2)
                   {
                       c1 = *(src+i + 2);
                       *(dst + pos) = c1;
                       i=i+1;
                   }
                   break;
                 default:
                   *(dst + pos) = c;
                   break;
             }
             i = i + 2;
          }
          else
          {
            i ++;
          }
      }
      else
      {
          *(dst + pos) = *(src+i);
          i ++;
      }
      pos ++;
   }
   *(dst+pos) = '\0';
   return pos;
}

ub1  getHexIndex(char c)
{
   if ( c >='0' && c <='9') return c - '0';
   if ( c >='a' && c <='f') return 10 + c - 'a';
   if ( c >='A' && c <='F') return 10 + c - 'A';
   return 0;
}

/* ----------------------------------------------------------------- */
/* Read lobs using stream mode into local buffers and then write     */
/* them to operating system files.                                   */
/* ----------------------------------------------------------------- */

void stream_read_clob(OCILobLocator *lobl, FILE *fp)
{
  ub4   offset = 1;
  ub4   loblen = 0;
  ub1   bufp[MAXBUFLEN];
  ub4   amtp = 0;
  sword retval;
  ub4   piece = 0;
  ub4   remainder;            /* the number of bytes for the last piece */

  OCILobGetLength(svchp, errhp, lobl, &loblen);
  amtp = loblen;

  //printf("--> To stream read LOB, loblen = %d.\n", loblen);

  memset(bufp, '\0', MAXBUFLEN);

  retval = OCILobRead(svchp, errhp, lobl, &amtp, offset, (dvoid *) bufp,
                     (loblen < MAXBUFLEN ? loblen : MAXBUFLEN), (dvoid *)0,
                     (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                     (ub2) 0, (ub1) SQLCS_IMPLICIT);

  switch (retval)
  {
    case OCI_SUCCESS:             /* only one piece */
      //printf("stream read %d th piece\n", ++piece);
      fwrite((void *)bufp, amtp, 1, fp);
      break;
    case OCI_ERROR:
      checkerr(errhp,retval);
      break;
    case OCI_NEED_DATA:           /* there are 2 or more pieces */
      remainder = loblen;
      fwrite((void *)bufp, amtp, 1, fp); /* full buffer to write */
      do
      {
        memset(bufp, '\0', MAXBUFLEN+1);
        amtp = 0;

        remainder -= MAXBUFLEN/2;

        retval = OCILobRead(svchp, errhp, lobl, &amtp, offset, (dvoid *) bufp,
                           (ub4) (remainder < MAXBUFLEN ? remainder : MAXBUFLEN), (dvoid *)0,
                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                           (ub2) 0, (ub1) SQLCS_IMPLICIT);

        /* the amount read returned is undefined for FIRST, NEXT pieces */
        //printf("stream read %d th piece, amtp = %d  rem=%d\n", ++piece, amtp,remainder);

        //if (remainder < MAXBUFLEN)     /* last piece not a full buffer piece */
        //   (void) fwrite((void *)bufp, (size_t)remainder, 1, fp);
        //else
        //   (void) fwrite((void *)bufp, MAXBUFLEN, 1, fp);
        fwrite((void *)bufp, amtp, 1, fp);
        

      } while (retval == OCI_NEED_DATA);
      break;
    default:
      printf("Unexpected ERROR: OCILobRead() LOB.\n");
      break;
  }
  return;
}

void stream_read_blob(OCILobLocator *lobl, FILE *fp)
{
  ub4   offset = 1;
  ub4   loblen = 0;
  ub1   bufp[MAXBUFLEN];
  ub4   amtp = 0;
  sword retval;
  ub4   piece = 0;
  ub4   remainder;            /* the number of bytes for the last piece */

  OCILobGetLength(svchp, errhp, lobl, &loblen);
  amtp = loblen;

  //printf("--> To stream read LOB, loblen = %d.\n", loblen);

  memset(bufp, '\0', MAXBUFLEN);

  retval = OCILobRead(svchp, errhp, lobl, &amtp, offset, (dvoid *) bufp,
                     (loblen < MAXBUFLEN ? loblen : MAXBUFLEN), (dvoid *)0,
                     (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                     (ub2) 0, (ub1) SQLCS_IMPLICIT);

  switch (retval)
  {
    case OCI_SUCCESS:             /* only one piece */
      printf("stream read %d th piece\n", ++piece);
      //fwrite((void *)bufp, (size_t)loblen, 1, fp);
      fwrite((void *)bufp, amtp, 1, fp);
      break;
    case OCI_ERROR:
      checkerr(errhp,retval);
      break;
    case OCI_NEED_DATA:           /* there are 2 or more pieces */
      remainder = loblen;
      fwrite((void *)bufp, MAXBUFLEN, 1, fp); /* full buffer to write */
      do
      {
        memset(bufp, '\0', MAXBUFLEN+1);
        amtp = 0;

        remainder -= MAXBUFLEN;

        retval = OCILobRead(svchp, errhp, lobl, &amtp, offset, (dvoid *) bufp,
                           (ub4) (remainder < MAXBUFLEN ? remainder : MAXBUFLEN), (dvoid *)0,
                           (sb4 (*)(dvoid *, const dvoid *, ub4, ub1)) 0,
                           (ub2) 0, (ub1) SQLCS_IMPLICIT);

        /* the amount read returned is undefined for FIRST, NEXT pieces */
        //printf("stream read %d th piece, amtp = %d  rem=%d\n", ++piece, amtp,remainder);

        //if (remainder < MAXBUFLEN)     /* last piece not a full buffer piece */
        //   (void) fwrite((void *)bufp, (size_t)remainder, 1, fp);
        //else
        //   (void) fwrite((void *)bufp, MAXBUFLEN, 1, fp);
        fwrite((void *)bufp, amtp, 1, fp);

      } while (retval == OCI_NEED_DATA);
      break;
    default:
      printf("Unexpected ERROR: OCILobRead() LOB.\n");
      break;
  }
  return;
}
