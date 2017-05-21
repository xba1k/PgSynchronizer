#include <stdlib.h>
#include "list.h"

list_item_t* listitem_new(void *data) {

    list_item_t *node = (list_item_t *) calloc(1, sizeof (list_item_t));
    if (node)
        node->data = data;
    return node;
}

void listitem_free(list_item_t **itemPtr) {
    if (itemPtr) {
        list_item_t *item = *itemPtr;
        if (item) {
            free(item);
            *itemPtr = NULL;
        }
    }
}

list_t *list_new(void) {
    list_t *list = (list_t *) calloc(1, sizeof (list_t));
    return list;
}

void list_free(list_t **list) {
    if (list && *list) {
        if (list_count(*list) > 0) {
            list_item_t *i;
            while ((i = list_pop(*list)) != NULL)
                listitem_free(&i);
        }
        free(*list);
        *list = NULL;
    }
}

void list_free2(list_t **list) {
    if (list && *list) {
        if (list_count(*list) > 0) {
            list_item_t *i;
            while ((i = list_pop(*list)) != NULL) {
                if(i->data)
                    free(i->data);
                listitem_free(&i);
            }
        }
        free(*list);
        *list = NULL;
    }
}

list_item_t *list_insert_after(list_t *list, list_item_t *new, list_item_t *after) {
    if (new) {

        after = after ? after : list->tail;
        if (after) {
            new->next = after->next;
            new->prev = after;
            after->next = new;
            if (new->next)
                new->next->prev = new;
            else
                list->tail = new;
        } else {
            list->head = list->tail = new;
        }
        list->count++;
    }
    return new;
}

list_item_t *list_insert(list_t *list, list_item_t *new) {
    return list_insert_after(list, new, NULL);
}

unsigned int list_count(list_t *list) {
    return list->count;
}

list_item_t *list_remove(list_t *list, list_item_t *old) {
    if (old) {
        if (old->next)
            old->next->prev = old->prev;
        else
            list->tail = old->prev;

        if (old->prev)
            old->prev->next = old->next;
        else
            list->head = old->next;

        list->count--;
    }
    return old;
}

list_item_t *list_pop(list_t *list) {
    return list_remove(list, list->head);
}

list_iterator_t * listiterator_new(list_t *list) {
    list_iterator_t *iter = NULL;
    if (list) {
        list_item_t *item = list_first(list);
        if (item) {
            iter = malloc(sizeof (list_iterator_t));
            iter->list = list;
            iter->item = item;
        }
    }
    return iter;
}

list_item_t *list_first(list_t *list) {
    return list->head;
}

list_item_t *list_next(list_item_t *node) {
    if (node) return node->next;
    return NULL;
}

list_item_t*listiterator_next(list_iterator_t *iter) {
    if (iter) {
        iter->item = list_next(iter->item);
        return iter->item;
    }
    return NULL;
}

list_item_t* listiterator_getitem(list_iterator_t *iter) {
    if (iter) {
        return iter->item;
    }
    return NULL;
}

void listiterator_free(list_iterator_t **iter) {
    if (iter && *iter) {
        free(*iter);
        *iter = NULL;
    }
}


