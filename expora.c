/*
   NAME
     ociuldr3.c - Using OCIStmtFetch2 etc function to rewrite unload script.

   MODIFIED   (MM/DD/YY) 
     Zhu Yi         2013.04.30 -  Initial rewrite.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <assert.h>

#include <string.h>


#include <oratypes.h>
#include <oci.h>
#include <ocidfn.h>
#include <ocidem.h>


#include <strings.h>
#define STRNCASECMP strncasecmp
 
void Checkerr(int line,OCIError *errhp,sword status);
#define checkerr(errhp,status)   Checkerr(__LINE__,errhp,status)

/* Constants used in this program. */
#define MAX_ITEM_BUFFER_SIZE    33
#define MAX_SELECT_LIST_SIZE    1024
#define MAXBUFLEN               5120
#define ROW_BATCH_SIZE          500000



#define  MIN(a,b) ((a) > (b) ? (b) : (a))

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


struct OCIColumn{
	text    colname[128+1];
  ub4   	colname_len;
  ub2   	coltype;
  ub2  		precision;
  ub4 	  colwidth;
  ub2    	scale;

  ub1     *colbuf;    //output variable
	size_t  colbuf_size;
  ub4 		buflen;
	ub2			*retlen;
	ub2			*retcode;
  OCIDefine       *dfnhp;
  OCILobLocator   *blob; //blob locator
  OCILobLocator   *clob; //clob locator
	sb2             *inq;  

};

typedef struct OCIColumn  	 OCIColumn;


struct OCIConn{
	OCISvcCtx    *svchp;

  OCIServer    *srvhp ;
  OCIError     *errhp ;
  OCISession   *sesshp; 
};


void OCICommit(struct OCIConn * conn){
	OCITransCommit(conn->svchp,conn->errhp,OCI_DEFAULT );
}

void dumpConn(struct OCIConn * conn){
	printf(" svchp:%p\n",conn->svchp);
	printf(" srvhp:%p\n",conn->srvhp);
	printf(" errhp:%p\n",conn->errhp);
	printf("sesshp:%p\n",conn->sesshp);

}

struct OCIStatement{
	OCIStmt  *  stmthp;
	struct OCIConn * conn;
	char     *   sql;
};

typedef struct OCIStatement  OCIStatement;
 

struct OCIResultSet {
		int colnum   ;  //
		int rownum   ;   //record the total row
		int totnum	 ;   //record the fetchd row num
		int arraysize;   //batchno fetch record
		struct OCIStatement *stmthp;
		OCIColumn   columns[];
	
};


struct OCIDrTable{
		OCIDirPathCtx *dpctx; 
		OCIDirPathFuncCtx *dpfnctx; 
		OCIDirPathColArray *dpca;  
		OCIDirPathStream *dpstr; 
		char tabname[128+1];

};

const char * getInertSql(struct OCIResultSet * reshp,const char * tabname){

	static char buffer[1024];
	memset(buffer,0x00,sizeof(buffer));
	sprintf(buffer,"insert into %s values(",tabname);
	for(int i=0;i<reshp->colnum;i++){
			char buf[32]="";
			sprintf(buf,":p%d",i+1);
			strcat(buffer,buf);
			if(i!=reshp->colnum-1)   strcat(buffer,",");
	}

	strcat(buffer,")");
	return buffer;
}

typedef struct OCIResultSet  OCIResultSet;

struct OCIConn *conn1=NULL;
struct OCIConn *conn2=NULL;

struct OCIConn* createConnection(char *v_user,char *v_pass,char *v_host);
struct OCIStatement  *  createStatement(struct OCIConn * conn,text * sql_statement);
sword executeStmt(struct OCIStatement *stmt,ub4 execount);
const char * OCI_getstring(OCIResultSet * reshp,int curidx);
OCIResultSet * executeQuery(struct OCIStatement *stmt,ub4 execount);
sword executeUpdate(struct OCIConn * conn,const char * sql);
void  printRowInfo(ub4 row);
int CursorNext(OCIResultSet * reshp);
void migrateTable(struct OCIConn *conn1,struct OCIConn *conn2,const char * tabname,const char *wherecond  );

/*Defined functions*/
void  initialize ();
void  logout();
void  cleanup();
void  freeColumn(struct COLUMN *col);
void printRow(text *fname,OCISvcCtx *svchp,struct OCIStatement *mystmt,struct COLUMN *col,text *field, int flen,text *record, int rlen, int batch, int header);
void  destr(char *src,char *v_user,char *v_pass,char *v_host);
void stream_read_clob(OCILobLocator *lobl, FILE *fp);
void stream_read_blob(OCILobLocator *lobl, FILE *fp);
sword preparSql(OCIStmt *stmhp, text *sql_statement);
sword executeSql(struct OCIConn *conn,OCIStmt *stmhp,ub4 execount);
sword getColumns(FILE *fpctl,struct OCIStatement  *stmt, struct COLUMN *collist);
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

ub4     DEFAULT_ARRAY_SIZE = 1000;
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
  text touser[132]="";
  text query[32768]="";
  text sqlfname[255]="";
  text tabname[132]="";
  text tabmode[132]="INSERT";
  text fname[255]="uldrdata.txt";
  text ctlfname[256]="";
  text field[132]=",";
  text logfile[256]="";
  text record[132]="\n";
  
  size_t  flen,rlen;
  int  buffer= 16777216;
  int  hsize = 0;
  int  ssize = 0;
  int  bsize = 0;
  int  serial= 0;
  int  trace = 0;
  int  batch = 0;
  int  header= 0;

  
  char p_user[50]="";
  char p_pass[50]="";
  char p_host[50]="";


  char p1_user[50]="";
  char p1_pass[50]="";
  char p1_host[50]="";
  
  FILE *fp=NULL;
  FILE *fpctl=NULL;
  
  flen = rlen = 1;
     
  for(i=0;i<argc;i++)
  {
    if (STRNCASECMP("user=",argv[i],5)==0)
    {
        memset(user,0,132);
        memcpy(user,argv[i]+5,MIN(strlen(argv[i]) - 5,131));
    }
    else if (STRNCASECMP("touser=",argv[i],7)==0)
    {
        memset(touser,0,132);
        memcpy(touser,argv[i]+7,MIN(strlen(argv[i]) - 7,131));
    }
    else if (STRNCASECMP("query=",argv[i],6)==0)
    {
        memset(query,0,8192);
        memcpy(query,argv[i]+6,MIN(strlen(argv[i]) - 6,8191));
    }
    else if (STRNCASECMP("table=",argv[i],6)==0)
    {
        memset(tabname,0,132);
        memcpy(tabname,argv[i]+6,MIN(strlen(argv[i]) - 6,128));
    }     
    else if (STRNCASECMP("batch=",argv[i],6)==0)
    {
        memset(tempbuf,0,1024);
        memcpy(tempbuf,argv[i]+6,MIN(strlen(argv[i]) - 6,254));
        batch = atoi(tempbuf);
        if (batch < 0) batch = 0;
        if (batch == 1) batch = 2;
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
       printf("ociuldr: Release 3.1 by Zhu yi\n");
       printf("\n");
       printf("Usage: %s user=... query=... field=... record=... file=...\n",argv[0]);
       printf("Notes:\n");
       printf("       user  = username/password@tnsname\n");
       printf("       touser= username/password@tnsname\n");
       printf("       query = select statement\n");
       printf("       table = table name in the sqlldr control file\n");
       printf("\n");
       exit(0);
    }
    else 
    { 	
      printf("Datauldr: Release 2.0\n");
      printf("\n");
  	  printf("Usage: %s user=... touser=... field=... record=... file=...\n",argv[0]);
  	  printf("More information please use:%s -help\n",argv[0]);
      printf("\n");
  	  exit(0);    
    }
  }		

  //de username,password,host

  initialize();

  destr(user,p_user,p_pass,p_host);
  printf("touser:%s %s %s\n",p_user,p_pass,p_host);

	conn1=createConnection(p_user,p_pass,p_host);
	if(conn1==NULL){ printf("conn1 connect failed\n"); }
	dumpConn(conn1);


	if(strlen(touser)>0){
  	destr(touser,p1_user,p1_pass,p1_host);
		printf("touser:%s %s %s\n",p1_user,p1_pass,p1_host);
		conn2=createConnection(p1_user,p1_pass,p1_host);
	if(conn2==NULL){ printf("conn2 connect failed\n"); }
	}
  
  
  /*prepary session env*/
 	executeUpdate(conn1,"ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD HH24:MI:SS'");

  executeUpdate(conn1,"ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SSXFF'");

  executeUpdate(conn1,"ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SSXFF TZH:TZM'");

  if (trace)
  {
	  memset(tempbuf,0,1024);
    sprintf(tempbuf,"ALTER SESSION SET EVENTS='10046 TRACE NAME CONTEXT FOREVER,LEVEL %d'", trace);
    executeUpdate(conn1,tempbuf);
  }

  /*prepary sql*/
	migrateTable(conn1,conn2,tabname,query);

	return 0;
	
	ub4 cot=0;
  
  logout();
  cleanup();
}



void migrateTable(struct OCIConn *conn1,struct OCIConn *conn2,const char * tabname,const char *wherecond  ){
	char buf[1024]="";
	sprintf(buf,"select *  from %s where 1=1 and %s",tabname,wherecond);

  struct OCIStatement  *mystmt=createStatement(conn1, buf); 

  OCIResultSet * rsthp=executeQuery(mystmt,0);

  struct OCIStatement  *mystmt1=createStatement(conn2, getInertSql(rsthp,tabname)); 
	printf("sql:%s\n",mystmt1->sql);	

	OCIStmt  *stmhp  =mystmt1->stmthp;
	OCIError *errhp  =conn2->errhp;

	int colnum=rsthp->colnum;
	
	struct OCIBind** bhp=malloc(colnum*sizeof(OCIBind*));
	printf("colnum:%d\n",colnum);
	int lastfetch=0;


	do{
		printf("haha:%d\n",colnum);
	 int rc=OCIFetch(rsthp);
	 printf("hehe:%d\n",colnum);
	 int rownum=rc;
	 if(rc<rsthp->arraysize) lastfetch=1;
	 printf("rownum %d\n",rownum);
	 if(rc==0){
		 printf("no data\n");
		 break;
	 }
		
		for(int i=0;i<colnum;i++){
			OCIColumn  * curcol=&rsthp->columns[i];
			ub1     *colbuf=curcol->colbuf;
		  ub4     colwidth=curcol->colwidth;
	
			rc=OCIBindByPos(stmhp, &bhp[i], errhp, i+1, colbuf, colwidth, SQLT_STR, curcol->inq, 0,0,0,0 , OCI_DEFAULT );
			if(rc!=0){
      	checkerr(errhp,rc);
			}
			//CheckErr(phErr,OCIBindByName(stmhp,&bndhp1,phErr,(text*)":1",(sb4)-1,(dvoid*)0,(sb4)0,SQLT_NTY,(dvoid*)0,(ub2*)0,(ub2*)0,(ub4)0,(ub4*)0,(ub4)OCI_DEFAULT));
		}

		OCISvcCtx    *svchp=conn2->svchp;
  
		//executeSql(conn2,stmhp,rownum);
	  //rc=OCIStmtExecute (svchp, stmhp, errhp, rownum, 0, NULL, NULL, OCI_DEFAULT);
	  //
	 	ub2 stmt_type = 0; 
	 	rc = OCIAttrGet(stmhp, OCI_HTYPE_STMT, &stmt_type, NULL, OCI_ATTR_STMT_TYPE, errhp);
    if (OCI_SUCCESS != rc) {
            printf("error : OCI_ATTR_STMT_TYPE");
            break;
    } 

	  rc=OCIStmtExecute (svchp, stmhp, errhp, rownum, 0, NULL, NULL, OCI_DEFAULT);
    checkerr(errhp,rc);

    //const char* str1=OCI_getstring(rst,1);
    //const char* str2=OCI_getstring(rst,2);
    //printf("%s %s\n",str1,str2);
  }while(lastfetch==0);
  
	OCICommit(conn2);

	//getInertSql();

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
    //	OCIDescriptorFree(p->clob,OCI_DTYPE_LOB);
    	
    
    temp=p;
    p=temp->next;
    free(temp);
  }
}

/* ----------------------------------------------------------------- */
/* retrieve error message and print it out.                          */
/* ----------------------------------------------------------------- */

void Checkerr(int line,OCIError *errhp,sword status)
{
  text errbuf[512];
  sb4 errcode = 0;

  switch (status)
  {
  case OCI_SUCCESS:
    break;
  case OCI_SUCCESS_WITH_INFO:
    //(void) printf("Error - OCI_SUCCESS_WITH_INFO\n");
    break;
  case OCI_NEED_DATA:
    (void) printf("%d Error - OCI_NEED_DATA\n",line);
    break;
  case OCI_NO_DATA:
    (void) printf("Error - OCI_NODATA\n");
    break;
  case OCI_ERROR:
    (void) OCIErrorGet ((dvoid *)errhp, (ub4) 1, (text *) NULL, &errcode,
                    errbuf, (ub4) sizeof(errbuf), OCI_HTYPE_ERROR);
    (void) printf("%d Error - %.*s\n", 512, line,errbuf);
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
/* output select result to files                                     */
/* ----------------------------------------------------------------- */
void dumpRow(text *fname,OCISvcCtx *svchp,struct OCIStatement *mystmt,struct COLUMN *col,text *field, int flen,text *record, int rlen, int batch, int header){
	OCIStmt  *stmhp  =mystmt->stmthp;
	OCIError *errhp  =mystmt->conn->errhp;
  ub4           numcols;
	ub2 type = 0;
	OCIParam *colhd ;
	
	OCIAttrGet((void *)stmhp, OCI_HTYPE_STMT, (void *)&numcols, (ub4 *)0, OCI_ATTR_PARAM_COUNT, errhp);

}

void printRow(text *fname,OCISvcCtx *svchp,struct OCIStatement *mystmt,struct COLUMN *col,text *field, int flen,text *record, int rlen, int batch, int header)
{

	OCIStmt  *stmhp  =mystmt->stmthp;
	OCIError *errhp  =mystmt->conn->errhp;

  ub4           colcount;
  ub4           tmp_rows;
  ub4           tmp_rows_size;
  ub4           rows;
  ub4           j;
  sword         rc;
  sword         r;
  ub4           c;
  ub4           trows;
  struct COLUMN *p=NULL;
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

    rc = OCIStmtFetch2(stmhp, errhp, DEFAULT_ARRAY_SIZE, OCI_DEFAULT, 0, OCI_DEFAULT);
    if (rc != 0)
    {
      if (rc!= OCI_NO_DATA)
      {
        return_code = 7;
        checkerr(errhp,rc);
      }	
      
      checkerr(errhp,OCIAttrGet((dvoid *) stmhp, (ub4) OCI_HTYPE_STMT,(dvoid *)&tmp_rows, (ub4 *) &tmp_rows_size, (ub4)OCI_ATTR_ROWS_FETCHED,errhp));
      rows = tmp_rows;
      //printf("rows is %d tmp_rows %d array size %d \n" , rows, tmp_rows, DEFAULT_ARRAY_SIZE);

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
  rc=OCIStmtPrepare(stmhp, errhp, (text *) sql_statement, (ub4) strlen(sql_statement), OCI_NTV_SYNTAX, OCI_DEFAULT);

  if (rc!=0)
  {
    checkerr(errhp,rc);
    return -1;
  }
  else
    return 0;
}

OCIResultSet 	*  executeQuery(struct OCIStatement *mystmt,ub4 execount){
	long defaultArraySize=300;
	OCIStmt  *stmhp  =mystmt->stmthp;
  OCIError *errhp  =mystmt->conn->errhp;
  ub4           numcols;       //select-list columns
	
	int rc=executeStmt(mystmt,execount);
	if(rc!=0){
      printf("executeUpdate %d\n",rc);
  }
	
	checkerr(errhp,OCIAttrGet(stmhp, OCI_HTYPE_STMT, &numcols,0, OCI_ATTR_PARAM_COUNT, errhp)); 
	size_t res_size=sizeof(OCIResultSet)+numcols*sizeof(OCIColumn);
	OCIResultSet * reshp=malloc(res_size );
	memset(reshp,0x00,res_size);
	if(reshp==NULL)  return NULL;
	reshp->colnum=numcols;
	reshp->rownum=0;
	reshp->totnum=0;
	reshp->stmthp=mystmt;
	reshp->arraysize=defaultArraySize;
	  /* Describe the select-list items. */
  for (int col = 0; col < numcols; col++)
  {
		OCIColumn  * curcol=&reshp->columns[col];

		OCIParam      *paramhp; 
		checkerr(errhp, OCIParamGet(stmhp, OCI_HTYPE_STMT, errhp, (void **)&paramhp, col+1));

		checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&curcol->coltype, 0, OCI_ATTR_DATA_TYPE, errhp));
		checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&curcol->colname, 0, (ub4)OCI_ATTR_NAME, errhp));
    checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&curcol->colwidth, 0, OCI_ATTR_DATA_SIZE, errhp));
    checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&curcol->precision, 0, OCI_ATTR_PRECISION, errhp));
    checkerr(errhp, OCIAttrGet(paramhp, OCI_DTYPE_PARAM,&curcol->scale, 0, OCI_ATTR_SCALE,errhp));

	  printf(" %d  width:%d\n",col,curcol->colwidth);

		size_t allocsize=defaultArraySize * (curcol->colwidth) ;
		curcol->colwidth++;  
		curcol->colbuf= malloc( allocsize );
		curcol->colbuf_size=allocsize;
		assert(curcol->colbuf!=NULL);
		memset(curcol->colbuf,0,allocsize);

		curcol->retlen =malloc(defaultArraySize* sizeof(sb2));
		assert(curcol->retlen!=NULL);
		curcol->retcode=malloc(defaultArraySize* sizeof(sb2));
		assert(curcol->retcode!=NULL);
	  curcol-> inq   = (sb2 *)malloc(defaultArraySize* sizeof(sb2));
		assert(curcol->inq!=NULL);
		

		checkerr(errhp,OCIDefineByPos(stmhp, &curcol->dfnhp, errhp, col+1, (dvoid *) curcol->colbuf,curcol->colwidth, SQLT_STR, curcol->inq, (ub2 *)curcol->retlen,(ub2 *)curcol->retcode, OCI_DEFAULT));

	}

	return reshp;

}


/*     0 lastfetch 1 ok  
*/    
int OCIFetch(OCIResultSet * reshp){
	int retcode=1;

	for(int i=0;i<reshp->colnum;i++ )	{
			OCIColumn  * curcol=&reshp->columns[i];
			memset(curcol->colbuf,0x00,curcol->colbuf_size);
	}
	
	OCIStatement  * mystmt=reshp->stmthp;
	OCIStmt  *stmhp  = mystmt->stmthp;
  OCIError *errhp  = mystmt->conn->errhp;
	int rows,tmp_rows,tmp_rows_size;
	printf("OCIFetch1:%d\n",reshp->arraysize);
	int rc = OCIStmtFetch2(stmhp, errhp, reshp->arraysize, OCI_DEFAULT, 0, OCI_DEFAULT);
	printf("OCIFetch2");
	//if(rc==OCI_NO_DATA){ checkerr(errhp,rc); }
	if(rc==OCI_NO_DATA){   }
  checkerr(errhp,OCIAttrGet((dvoid *) stmhp, (ub4) OCI_HTYPE_STMT,(dvoid *)&tmp_rows, (ub4 *) &tmp_rows_size, (ub4)OCI_ATTR_ROWS_FETCHED,errhp));
 	rows = tmp_rows;
	reshp->totnum+=rows;
	return rows;
}


const char * OCI_getstring(OCIResultSet * reshp,int curidx){
		/*
		if(reshp->totnum==reshp->rownum) {
			int rc=OCIFetch(reshp);
			if(rc==0)   return "";
		}
		
		*/

		OCIColumn  * curcol=&reshp->columns[curidx];
		ub1     *colbuf=curcol->colbuf;
		ub4     colwidth=curcol->colwidth;
		int batchidx= (reshp->rownum-1)%reshp->totnum;
		return &colbuf[ batchidx* colwidth ];
}


//  0  not record ,-1  error   1=found
int CursorNext(OCIResultSet * reshp){
	int lastfetch=0;
	if(reshp->rownum==reshp->totnum  && lastfetch==0) {  //try to fetch 
			size_t rows=OCIFetch(reshp);
			if(rows<reshp->arraysize)  lastfetch=1;
	}

	if(reshp->rownum<reshp->totnum)  {
		reshp->rownum++;
		return 1;
	}
	return 0;
}


/* ----------------------------------------------------------------- */
/* execute a sql statement                                           */
/* ----------------------------------------------------------------- */
sword executeStmt(struct OCIStatement *stmt,ub4 execount){
	
	if(stmt==NULL)  return -1;
	OCISvcCtx *svchp=stmt->conn->svchp;
	OCIError *errhp =stmt->conn->errhp;
  OCIStmt   *stmhp=stmt->stmthp;
  sword rc;
  rc=OCIStmtExecute(svchp,stmhp,errhp,execount,0,NULL,NULL,OCI_DEFAULT);
	if(rc!=0){
			checkerr(stmt->conn->errhp,rc);
			return rc;
	}
	return 0;
}


sword executeUpdate(struct OCIConn * conn,const char * sql){
	OCIError *errhp =conn->errhp;
  struct OCIStatement * stmt1=createStatement(conn,sql);
	if(stmt1==NULL){
			printf("stmt1 error1\n");
	}
	int rc=executeStmt(stmt1,1);
	if(rc!=0){
			printf("executeUpdate %d\n",rc);
   	  checkerr(errhp,rc);
	}
	
	return rc;
}


sword executeSql(struct OCIConn *conn,OCIStmt *stmhp,ub4 execount)
{

	OCISvcCtx *svchp=conn->svchp;
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


struct OCIConn* createConnection(char *v_user,char *v_pass,char *v_host){
    struct OCIConn * conn=malloc(sizeof(struct OCIConn));
		 memset(  conn,0x00,sizeof(struct OCIConn));
     OCIServer    *srvhp ;
     OCIError     *errhp ;
     OCISession   *sesshp;
		 OCISvcCtx    *svchp;
		int rc=0;

    rc=OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &errhp, OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0);
		assert(rc==0);
    rc=OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &srvhp, OCI_HTYPE_SERVER, (size_t) 0, (dvoid **) 0);
		assert(rc==0);
		rc=OCIHandleAlloc ((dvoid *) envhp, (dvoid **) &svchp, OCI_HTYPE_SVCCTX, (size_t) 0, (dvoid **) 0);
		assert(rc==0);
    rc=OCIHandleAlloc ((dvoid *) envhp, (dvoid **)&sesshp, OCI_HTYPE_SESSION, (size_t) 0, (dvoid **) 0);
		assert(rc==0);

    OCIAttrSet ((dvoid *) svchp, OCI_HTYPE_SVCCTX, (dvoid *)srvhp, (ub4) 0, OCI_ATTR_SERVER, (OCIError *) errhp);
    if (strlen(v_host)==0)
    {
        checkerr(errhp,OCIServerAttach (srvhp,errhp,(text *)0,(sb4)0,OCI_DEFAULT));
    }       
    else
        {
        if (OCIServerAttach (srvhp,errhp,(text *)v_host,(sb4)strlen(v_host),OCI_DEFAULT)==OCI_SUCCESS)
        {
          printf ("Connect to %s sucessful!\n",v_host);
        }
        else
        {
         printf ("Connect to %s failed!\n",v_host);
        }
    } 
    
		OCIAttrSet ((dvoid *)sesshp, (ub4)OCI_HTYPE_SESSION, (dvoid *)v_user, (ub4)strlen((char *)v_user), OCI_ATTR_USERNAME, errhp);
   	OCIAttrSet ((dvoid *)sesshp, (ub4)OCI_HTYPE_SESSION, (dvoid *)v_pass, (ub4)strlen((char *)v_pass), OCI_ATTR_PASSWORD, errhp);

		checkerr (errhp, OCISessionBegin (svchp,  errhp, sesshp, OCI_CRED_RDBMS,(ub4) OCI_DEFAULT));

   	OCIAttrSet ((dvoid *) svchp, (ub4) OCI_HTYPE_SVCCTX, (dvoid *) sesshp, (ub4) 0, (ub4) OCI_ATTR_SESSION, errhp);
    conn->srvhp=srvhp;
    conn->errhp=errhp;
    conn->sesshp=sesshp; 
		conn->svchp=svchp;
        
   	return conn;
}

struct OCIStatement  *  createStatement(struct OCIConn * conn,text * sql_statement){
	struct OCIStatement * stmt=malloc(sizeof(struct OCIStatement ));
	OCIStmt *stmhp=NULL;
	stmt->sql=strdup(sql_statement);
	stmt->conn=conn;
	
  sword rc=0;
  OCIError  *errhp =conn->errhp;

  rc=OCIHandleAlloc((dvoid *) envhp, (dvoid **) &stmhp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
	if(rc!=0){
			printf("alloc error\n");
	}
	
  rc=OCIStmtPrepare(stmhp, errhp, (text *) sql_statement, (ub4) strlen(sql_statement), OCI_NTV_SYNTAX, OCI_DEFAULT);
  if (rc!=0)
  {
    checkerr(errhp,rc);
    return NULL;
  }
	stmt->stmthp=stmhp;
  return stmt;

}
