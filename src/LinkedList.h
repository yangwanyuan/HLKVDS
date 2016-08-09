#ifndef _KV_DB_LINKEDLIST_H_
#define _KV_DB_LINKEDLIST_H_

#include <string.h>
#include <unistd.h>
#include <vector>
//#include <string>

using namespace std;
namespace kvdb{

    template <typename T> 
    class Node{
    public:
        T data;
        Node<T>* next;

        Node(T value, Node<T>* nd): data(value), next(nd){};
    };


    template <typename T>
    class LinkedList{
    //public:
        //class Node{
        //public:
        //    T data;
        //    Node* next;

        //    Node(T value, Node* nd): data(value), next(nd){};
        //};

    public:
        LinkedList(): m_head(NULL), m_size(0){}
        LinkedList(const LinkedList<T> &);
        ~LinkedList();
        LinkedList& operator= (const LinkedList<T> &);
        bool insert(T toBeInserted);
        bool remove(T toBeRemoved);
        bool search(T toBeSearched);
        vector<T> get();
        int get_size() {return m_size;}

    private:
        Node<T>* m_head;
        int m_size;

        void copyHelper(const LinkedList &);
        void removeAll();


    };

    //template <typename T>
    //LinkedList<T>::LinkedList()
    //{
    //    m_head = NULL;
    //    m_size = 0;
    //}

    template <typename T>
    LinkedList<T>::LinkedList(const LinkedList<T> &toBeCopied)
    {
        copyHelper(toBeCopied);
    }

    template <typename T>
    LinkedList<T>::~LinkedList()
    {
        removeAll();
    }

    template <typename T>
    LinkedList<T> &LinkedList<T>::operator= (const LinkedList<T> &toBeCopied)
    {
        // Only do if the parameter is not the calling object 
        if (this != &toBeCopied)
        {
            removeAll();
            copyHelper(toBeCopied);
        }
        return *this;
    }

    template <typename T>
    void LinkedList<T>::copyHelper(const LinkedList<T> &toBeCopied)
    {   
        // If the parameter list is empty
        if (toBeCopied.m_head == NULL)
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
            Node<T>* copyNode = new Node<T>(toBeCopied.m_head->data, NULL);
            m_head = copyNode;
    
            Node<T>* ptr = toBeCopied.m_head;
            ptr = ptr->next;
            // Copy the rest of the list until the tail
            while (ptr != NULL)
            {
                copyNode->next = new Node<T>(ptr->data, NULL);
                copyNode = copyNode->next;
                ptr = ptr->next;
            }
        }
    
    }

    template <typename T>
    void LinkedList<T>::removeAll()
    {
        Node<T>* tempNode = m_head;
        // De-allocate all the memory associated with the calling list from the head to tail
        while (tempNode != NULL)
        {
            tempNode = m_head->next;
            delete m_head;
            m_head = tempNode;
        }
        // Also set the size of the list to zero
        m_size = 0;
    
    }

    template <typename T>
    bool LinkedList<T>::insert(T toBeInserted)
    {
        // If the string is already in the list
        if (search(toBeInserted))
        {
            return false;
        }
        else    // Else if the string is not in the list
        {
            // Create a new node of that string
            Node<T>* newNode = new Node<T>(toBeInserted, NULL);
    
            //If the list is empty
            if (m_head == NULL)
            {
                m_head = newNode;
            }
            else    // Else if the list is not empty
            {
                //newNode->next = m_head;
                //m_head = newNode;
                Node<T>* tempNode = m_head;
                while (tempNode->next != NULL)
                    tempNode = tempNode->next;
                tempNode->next = newNode;
            }
            m_size++;
            return true;
        }
    }

    template <typename T>
    bool LinkedList<T>::remove(T toBeRemoved)
    {   
        // If string does not exist in the list
        if (!search(toBeRemoved))
        {
            return false;
        }
        else    // String exist
        {
            // Node initialisation (so can remove and point the right one)
            Node<T>* nodePtr = m_head;
            Node<T>* tempNode = nodePtr->next;
    
            //If the head is the node we want to remove
            if (m_head->data == toBeRemoved)
            {
                m_head = tempNode;
                delete nodePtr;
                m_size--;
                return true;
            }
            else
            {
                // Find the one user want to remove till the tail
                while (tempNode != NULL)
                {
                    //If we found the node
                    if (tempNode->data == toBeRemoved)
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

    template <typename T>
    bool LinkedList<T>::search(T toBeSearched) 
    {
        Node<T>* tempNode = m_head;
        
        //Check until the last node in list
        while (tempNode != NULL)
        {
            // If the current node contain the data that user wanted to check
            if (tempNode->data == toBeSearched)
            {
                return true;
            }
            tempNode = tempNode->next;
    
        }
        // Only return false if data not found after program had went thru the whole list
        return false;
    
    }

    template <typename T>
    vector<T> LinkedList<T>::get() 
    {
        vector<T> tempVector;
        if (m_head != NULL)
        {
            Node<T>* tempNode = m_head;
            // Till the last node
            while (tempNode != NULL)
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

#endif
