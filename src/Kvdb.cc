#include "Kvdb.h"
#include "Kvdb_Impl.h"

namespace kvdb{

DB *DB::m_db = NULL;

bool DB::CreateDB(string filename, 
                uint64_t hash_table_size,
                uint64_t segment_size){
        
    KvdbDS* kvdb_db;
    kvdb_db = KvdbDS::Create_KvdbDS(filename.c_str(), 
                                                hash_table_size, 
                                                //max_deleted_ratio, 
                                                //max_load_factor,
                                                segment_size);
    if (!kvdb_db)
    {
        std::cout << "CreateDB failed" << std::endl;
        return false;
    }
    if (!kvdb_db->WriteMetaDataToDevice())
    {
        std::cout << "CreateDB write metadata to file Failed!" << std::endl;
        return false;
    }    
   
    return true;
}

bool DB::OpenDB(string filename, DB** db){
    //if (m_db != NULL){
    //      std::cout << "DB is exist, Please call GetDB" << std::endl;
    //      return false;
    //}
    if (m_db == NULL) 
    {
        m_db = new DB();
    }

    m_db->m_kvdb_db = KvdbDS::Open_KvdbDS(filename.c_str());
    if (!m_db->m_kvdb_db)
    {
        std::cout << "OpenDB failed" <<std::endl;
        return false;
    }

    *db = m_db;
    //std::cout << "OpenDB success" <<std::endl;
    return true;
}

//DB* DB::GetDB(){
//      if (m_db == NULL){
//              std::cout << "Please OpenDB first" << std::endl;
//              return NULL;
//      }
//
//      std::cout << "GetDB success" << std::endl;
//      return m_db;
//}

bool DB::WriteIndexToFile(){
    if (!m_kvdb_db->WriteMetaDataToDevice())
    {
        std::cout << "WriteIndexToFile failed" << std::endl;
        return false;
    }
    //std::cout << "WriteIndexToFile success" <<std::endl;
    return true;
}

DB::DB(){
    ;
    //std::cout << "DB init success" <<std::endl;
}

DB::~DB(){
    WriteIndexToFile();
    //std::cout << "DB destory success" <<std::endl;
}

bool DB::Insert(const char* key, uint32_t key_len,const char* data, uint32_t length){
    if (!m_kvdb_db->Insert(key, key_len, data, length))
    {
        std::cout << "DB Insert failed" <<std::endl;
        return false;
    }
    //std::cout << "DB Insert success" <<std::endl;
    return true;
}

bool DB::Delete(const char* key, uint32_t key_len){
    if (!m_kvdb_db->Delete(key, key_len))
    {
        std::cout << "DB Delete failed" <<std::endl;  
        return false;
    }
    //std::cout << "DB Delete success" <<std::endl;
    return true;
}

bool DB::Get(const char* key, uint32_t key_len, string &data){
    if (!m_kvdb_db->Get(key, key_len, data))
    {
        std::cout << "DB Get failed" <<std::endl; 
        return false;
    }
    //std::cout << "DB Get success" <<std::endl;
    return true;
}

}// end namespace kvdb
