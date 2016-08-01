#include "LinkedList.h"

namespace kvdb{
    LinkedList::LinkedList()
    {
        m_head = NULL;
        m_size = 0;
    }

    LinkedList::LinkedList(const LinkedList &toBeCopied)
    {
        copyHelper(toBeCopied);
    }

    LinkedList::~LinkedList()
    {
        removeAll();
    }

    LinkedList &LinkedList::operator= (const LinkedList &toBeCopied)
    {
        // Only do if the parameter is not the calling object 
        if(this != &toBeCopied)
        {
            removeAll();
            copyHelper(toBeCopied);
        }
        return *this;
    }

    void LinkedList::copyHelper(const LinkedList &toBeCopied)
    {   
        // If the parameter list is empty
        if(toBeCopied.m_head == NULL)
        {
            // Create an empty list
            m_head = NULL;
            m_size = 0;
        }
        else
        {
            // Deep copy the contents of the parameter list
            m_size = toBeCopied.m_size;
            // Copy the first node first
            EntryNode* copyNode = new EntryNode(toBeCopied.m_head->data, NULL);
            m_head = copyNode;
    
            EntryNode* ptr = toBeCopied.m_head;
            ptr = ptr->next;
            // Copy the rest of the list until the tail
            while(ptr != NULL)
            {
                copyNode->next = new EntryNode(ptr->data, NULL);
                copyNode = copyNode->next;
                ptr = ptr->next;
            }
        }
    
    }

    void LinkedList::removeAll()
    {
        EntryNode* tempNode = m_head;
        // De-allocate all the memory associated with the calling list from the head to tail
        while(tempNode != NULL)
        {
            tempNode = m_head->next;
            delete m_head;
            m_head = tempNode;
        }
        // Also set the size of the list to zero
        m_size = 0;
    
    }

    bool LinkedList::insert(HashEntry toBeInserted)
    {
        // If the string is already in the list
        if(search(toBeInserted))
        {
            //return false;
            remove(toBeInserted);
            // Create a new node of that string
            EntryNode* newNode = new EntryNode (toBeInserted, NULL);
    
            //If the list is empty
            if(m_head == NULL)
            {
                m_head = newNode;
            }
            else    // Else if the list is not empty
            {
                newNode->next = m_head;
                m_head = newNode;
            }
            m_size++;
            return true;
        }
        else    // Else if the string is not in the list
        {
            // Create a new node of that string
            EntryNode* newNode = new EntryNode (toBeInserted, NULL);
    
            //If the list is empty
            if(m_head == NULL)
            {
                m_head = newNode;
            }
            else    // Else if the list is not empty
            {
                newNode->next = m_head;
                m_head = newNode;
            }
            m_size++;
            return true;
        }
    }

    bool LinkedList::remove(HashEntry toBeRemoved)
    {   
        // If string does not exist in the list
        if(!search(toBeRemoved))
        {
            return false;
        }
        else    // String exist
        {
            // Node initialisation (so can remove and point the right one)
            EntryNode* nodePtr = m_head;
            EntryNode* tempNode = nodePtr->next;
    
            //If the head is the node we want to remove
            if(!memcmp(&(m_head->data.hash_entry.dataheader.key), &(toBeRemoved.hash_entry.dataheader.key), 20))
            {
                m_head = tempNode;
                delete nodePtr;
                m_size--;
                return true;
            }
            else
            {
                // Find the one user want to remove till the tail
                while(tempNode != NULL)
                {
                    //If we found the node
                    if(!memcmp(&(m_head->data.hash_entry.dataheader.key), &(toBeRemoved.hash_entry.dataheader.key), 20))
                    {
                        // Point the node before the current to the node after the current
                        nodePtr->next = tempNode->next;
                        delete tempNode;
                        m_size--;
                        return true;
    
                    }
                    // Keep going the list insequence
                    nodePtr = nodePtr->next;
                    tempNode = tempNode->next;
                }
            }
        }
        // If not there, not possible tho, cause checked before 
        return false;
    }

    bool LinkedList::search(HashEntry toBeSearched) 
    {
        EntryNode* tempNode = m_head;
        
        //Check until the last node in list
        while(tempNode != NULL)
        {
            // If the current node contain the data that user wanted to check
            if(!memcmp(&(tempNode->data.hash_entry.dataheader.key), &(toBeSearched.hash_entry.dataheader.key), 20))
            {
                return true;
            }
            tempNode = tempNode->next;
    
        }
        // Only return false if data not found after program had went thru the whole list
        return false;
    
    }

    vector<HashEntry> LinkedList::get() 
    {
        vector<HashEntry> tempVector;
        if(m_head != NULL)
        {
            EntryNode* tempNode = m_head;
            // Till the last node
            while(tempNode != NULL)
            {
                // Push back the contents in the vector
                tempVector.push_back(tempNode->data);
                // Move on to the  next node
                tempNode = tempNode->next;
            }
        }
        return tempVector;
    } 


}
