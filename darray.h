#ifndef __DARRAY_H_
#define __DARRAY_H_

typedef long u64;

enum {
    DARRAY_CAPACITY,
    DARRAY_LENGTH,
    DARRAY_STRIDE, // Size of each element in array
    DARRAY_FIELD_LENGTH,
};

void *_darray_create(u64 length, u64 stride);
void _darray_destroy(void *array);

u64 _darray_field_get(void *array, u64 field);
void _darray_field_set(void *array, u64 field, u64 value);

void *_darray_resize(void *array);

void *_darray_push(void *array, const void *value_ptr);
void *_darray_push_sized(void *array, const void *value_ptr, u64 size);
void _darray_pop(void *array, void *dest);

void *_darray_pop_at(void *array, u64 index, void *dest);
void *_darray_insert_at(void *array, u64 index, const void *value_ptr);

void *darray_last(void *array);

#define DARRAY_DEFAULT_CAPACITY 30
#define DARRAY_RESIZE_FACTOR 2

#define darray_create(type) _darray_create(DARRAY_DEFAULT_CAPACITY, sizeof(type))
#define darray_reserve(type, capacity) _darray_create(capacity, sizeof(type))

#define darray_destroy(array) _darray_destroy(array)

// #define darray_push(array, value)            \
//     {                                        \
//         typeof(value) temp = value;          \
//         array = _darray_push(array, &temp);  \
//     }

#define darray_push(array, value)                       \
    do {                                                \
        typeof(value) temp = value;                     \
        u64 size = sizeof(temp);                        \
        array = _darray_push_sized(array, &temp, size); \
    } while (0)

#define darray_clear(array) _darray_field_set(array, DARRAY_LENGTH, 0)

#define darray_capacity(array) _darray_field_get(array, DARRAY_CAPACITY)

#define darray_length(array) _darray_field_get(array, DARRAY_LENGTH)

#define darray_stride(array) _darray_field_get(array, DARRAY_STRIDE)

#define darray_length_set(array, value) _darray_field_set(array, DARRAY_LENGTH, value)

#endif // __DARRAY_H_

#ifdef DARRAY_IMPLEMENTATION

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define KERROR(message, ...) do { \
    printf("ERROR: file: %s, line: %d", __FILE__, __LINE__); \
    printf(message, __VA_ARGS__); \
} while(0)

void *_darray_create(u64 length, u64 stride) {
    u64 header_size = DARRAY_FIELD_LENGTH * sizeof(u64);
    u64 array_size = stride * length;

    u64 *new_array = malloc(header_size + array_size);

    new_array[DARRAY_CAPACITY] = length;
    new_array[DARRAY_LENGTH] = 0;
    new_array[DARRAY_STRIDE] = stride;

    return (void *)(new_array + DARRAY_FIELD_LENGTH);
}

void _darray_destroy(void *array) {
    u64 *header = ((u64 *) array) - DARRAY_FIELD_LENGTH;

    u64 header_size = DARRAY_FIELD_LENGTH * sizeof(u64);
    u64 total_size = header_size + header[DARRAY_CAPACITY] * header[DARRAY_STRIDE];

    free(header);
}


u64 _darray_field_get(void *array, u64 field) {
    u64 *header = ((u64 *) array) - DARRAY_FIELD_LENGTH;
    return header[field];
}

void _darray_field_set(void *array, u64 field, u64 value) {
    u64 *header = ((u64 *) array) - DARRAY_FIELD_LENGTH;
    header[field] = value;
}

void *darray_last(void *array) {
    u64 stride = darray_stride(array);
    u64 length = darray_length(array);

    if (length == 0) {
        return NULL;
    }

    u64 addr = (u64) array;
    addr += (length - 1) * stride;

    return (void *) addr;
}

void *_darray_resize(void *array) {
    u64 length = darray_length(array);
    u64 stride = darray_stride(array);

    u64 new_length = DARRAY_RESIZE_FACTOR * darray_capacity(array);

    void *temp = _darray_create(new_length, stride);
    memcpy(temp, array, length * stride);

    _darray_field_set(array, DARRAY_LENGTH, length);
    _darray_destroy(array);

    return temp;
}

void *_darray_push(void *array, const void *value_ptr) {
    u64 length = darray_length(array);
    u64 stride = darray_stride(array);

    if (length >= darray_capacity(array)) {
        array = _darray_resize(array);
    }

    u64 addr = (u64) array;
    addr += length * stride;

    memcpy((void *) addr, value_ptr, stride);
    _darray_field_set(array, DARRAY_LENGTH, length + 1);

    return array;
}

void *_darray_push_sized(void *array, const void *value_ptr, u64 data_size) {
    u64 length = darray_length(array);
    u64 stride = darray_stride(array);

    if (length >= darray_capacity(array)) {
        array = _darray_resize(array);
    }

    u64 addr = (u64) array;
    addr += length * stride;

    if (data_size > stride) {
        data_size = stride;
    }

    memcpy((void *) addr, value_ptr, data_size);
    _darray_field_set(array, DARRAY_LENGTH, length + 1);

    return array;
}
#endif // DARRAY_IMPLEMENTATION
