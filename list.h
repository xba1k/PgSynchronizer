#ifndef LIST_H
#define	LIST_H

#ifdef	__cplusplus
extern "C" {
#endif

#include <stdlib.h>
    
typedef struct _listItem {
        void *data;
        struct _listItem *next;
        struct _listItem *prev;
} list_item_t;

typedef struct {

        list_item_t *head;
        list_item_t *tail;
        unsigned int count;
        
} list_t;

typedef struct _listIterator {

        list_item_t *item;
        list_t *list;

} list_iterator_t;

list_item_t* listitem_new(void *data);
void listitem_free(list_item_t **itemPtr);
list_t *list_new(void);
void list_free(list_t **list);
void list_free2(list_t **list);
list_item_t *list_insert_after(list_t *list, list_item_t *new, list_item_t *after);
list_item_t *list_insert(list_t *list, list_item_t *new);
unsigned int list_count(list_t *list);
list_item_t *list_remove(list_t *list, list_item_t *old);
list_item_t *list_pop(list_t *list);
unsigned int list_count(list_t *list);

list_iterator_t * listiterator_new(list_t *list);
list_item_t *list_first(list_t *list);
list_item_t *list_next(list_item_t *node);
list_item_t*listiterator_next(list_iterator_t *iter);
list_item_t* listiterator_getitem(list_iterator_t *iter);
void listiterator_free(list_iterator_t **iter);

#ifdef	__cplusplus
}
#endif

#endif	/* LIST_H */

