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
    public:
        LinkedList(): head_(NULL), size_(0){}
        LinkedList(const LinkedList<T> &);
        ~LinkedList();
        LinkedList& operator=(const LinkedList<T> &);
        bool insert(T toBeInserted);
        bool remove(T toBeRemoved);
        bool search(T toBeSearched);
        vector<T> get();
        int get_size() { return size_; }

    private:
        Node<T>* head_;
        int size_;

        void copyHelper(const LinkedList &);
        void removeAll();


    };

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
    LinkedList<T> &LinkedList<T>::operator=(const LinkedList<T> &toBeCopied)
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
        if (toBeCopied.head_ == NULL)
        {
            // Create an empty list
            head_ = NULL;
            size_ = 0;
        }
        else
        {
            // Deep copy the contents of the parameter list
            size_ = toBeCopied.size_;
            // Copy the first node first
            Node<T>* copyNode = new Node<T>(toBeCopied.head_->data, NULL);
            head_ = copyNode;
    
            Node<T>* ptr = toBeCopied.head_;
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
        Node<T>* tempNode = head_;
        // De-allocate all the memory associated with the calling list from the head to tail
        while (tempNode != NULL)
        {
            tempNode = head_->next;
            delete head_;
            head_ = tempNode;
        }
        // Also set the size of the list to zero
        size_ = 0;
    
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
            if (head_ == NULL)
            {
                head_ = newNode;
            }
            else    // Else if the list is not empty
            {
                //newNode->next = head_;
                //head_ = newNode;
                Node<T>* tempNode = head_;
                while (tempNode->next != NULL)
                    tempNode = tempNode->next;
                tempNode->next = newNode;
            }
            size_++;
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
            Node<T>* nodePtr = head_;
            Node<T>* tempNode = nodePtr->next;
    
            //If the head is the node we want to remove
            if (head_->data == toBeRemoved)
            {
                head_ = tempNode;
                delete nodePtr;
                size_--;
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
                        size_--;
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
        Node<T>* tempNode = head_;
        
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
        if (head_ != NULL)
        {
            Node<T>* tempNode = head_;
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
