#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <cassert>
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>

namespace SpatialStorage {

    const uint64_t INVALID_ROOT_ADDR = 0;

    template <typename KeyT>
    struct KeyValuePair {
        KeyT            key;
        const void      *value;
    };

    class IndexHeader {
        public:
            uint64_t    Dimensions;
            uint64_t    key_size;
            uint64_t    value_size;
            uint64_t    block_size;
            uint64_t    root_addr;

    };

    enum class BlockType {
        LeafBlock=0,
        InnerBlock
    };

    class NodeHeader {
        private:
            BlockType   block_type_;
            uint64_t    entry_count_{0};
            uint64_t    in_file_addr_{INVALID_ROOT_ADDR};

        public:
            NodeHeader(BlockType blocktype,uint64_t entry_count,uint64_t in_file_addr)
                :block_type_{blocktype}, entry_count_{entry_count}, in_file_addr_{in_file_addr}
            {}

            auto GetEntryCount()->uint64_t { return entry_count_; }
            auto SetEntryCount(uint64_t entry_count)->void { entry_count_ = entry_count; }
            auto IsLeafBlock()->bool { return block_type_==BlockType::LeafBlock; }
            auto SetBlockType(BlockType bt)->void { block_type_ = bt; }
            auto GetInFileAddr()->uint64_t { return in_file_addr_; }
            auto SetInFileAddr(uint64_t in_file_addr)->void { in_file_addr_ = in_file_addr; }
    };

    template<typename KeyT>
    class NodeHandler {
        private:
            NodeHeader* header_;
            uint64_t    key_size_;
            uint64_t    value_size_;
            uint64_t    block_size_;
            uint64_t    dimensions_;

        public:
            NodeHandler(NodeHeader *header,size_t key_size,size_t value_size,size_t block_size)
                : header_{header}, key_size_{key_size}, value_size_{value_size}, block_size_(block_size)
            {}

            NodeHeader *get_header() {return header_;}
            void set_header(NodeHeader* header) {header_ = header;}

            bool IsLeafBlock() { return header_->IsLeafBlock(); } 
            void SetBlockType(BlockType bt) {header_->SetBlockType(bt);}

            uint64_t get_in_file_addr() { return header_->GetInFileAddr(); }
            void set_in_file_addr(uint64_t ifa) {header_->SetInFileAddr(ifa);}

            uint64_t get_count() { return header_->GetEntryCount(); }
            void set_count(uint64_t count) { header_->SetEntryCount(count); }

            uint64_t get_pair_size() const { return key_size_+value_size_; }

            uint64_t get_entry_capacity() const {
                return (block_size_-sizeof(NodeHeader)) / get_pair_size();
            }

            bool is_full() { return header_->GetEntryCount() >= get_entry_capacity(); }

            KeyT *get_elem_key(uint64_t idx) {
                void* res = nullptr;
                res = reinterpret_cast<uint8_t*>(header_) + sizeof(NodeHeader) + idx * get_pair_size();

                return reinterpret_cast<KeyT *>(res);
            }

            void *get_elem_value(uint64_t idx) {
                return reinterpret_cast<uint8_t *>(header_) + sizeof(NodeHeader) + (idx * get_pair_size() + key_size_);
            }

            void *get_elem_ptr(uint64_t idx) {
                // assert(IsLeafBlock());
                return reinterpret_cast<uint8_t *>(header_) + sizeof(NodeHeader) + idx * get_pair_size();
            }

            KeyValuePair<KeyT> *get_elem_pair(uint64_t idx) {
                auto ptr = get_elem_ptr(idx);
                return KeyValuePair<KeyT>{*get_elem_key(idx),get_elem_value(idx)};
                // return static_cast<const KeyValuePair<KeyT> *>(ptr);
            }

            void set_elem_key(KeyT *modify_key,uint64_t idx) {
                KeyT *key_ptr = get_elem_key(idx);
                *key_ptr = *modify_key;
            }

            void delete_elem_key(uint64_t idx){
                auto count = get_count();
                for(uint64_t i=idx+1;i<count;i++){
                    auto key_ptr = get_elem_key(i);
                    auto pre_key_ptr = get_elem_key(i-1);
                    memmove(
                        reinterpret_cast<uint8_t *>(pre_key_ptr),key_ptr,get_pair_size()
                    )
                }
                set_count(count);
            }

            void insert(KeyValuePair<KeyT>& kvp) {
                auto capacity = get_entry_capacity();
                auto entry_count = get_count();
                assert(entry_count<capacity);

                auto new_kptr = get_elem_key(entry_count);
                *new_kptr = kvp.key;
                memcpy(reinterpret_cast<uint8_t*>(new_kptr)+sizeof(KeyT),kvp.value,value_size_);
                set_count(entry_count+1);
            }

            void clear() {
                set_count(0);
            }
    };
}

#endif