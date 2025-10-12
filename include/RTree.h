#ifndef R_TREE_H
#define R_TREE_H

#include <cassert>
#include <cstring>
#include <iostream>
#include <optional>
#include <deque>
#include <list>
#include <queue>
#include <fcntl.h>

#include <unistd.h>
#include <zconf.h>

#include "FileCache.h"
#include "Node.h"
#include "Type.h"

namespace SpatialStorage {
    const uint64_t PAGE_UNIT = 0x1000;
    const uint64_t INDEX_HEADER_ADDR = 0x1000;

    enum class SearchMode {
        overlap=0,
        comprise
    };

    template<typename KeyT>
    class Context {
        public:
            std::deque<NodeHandler<RKeyType<KeyT>>> path;
    };

    template<typename KeyT>
    class RTree {
        public:
            static RTree<KeyT> create(
                int dir,
                const char *name,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions) 
            {
                int index = openat(dir, name, O_RDWR | O_CREAT | O_EXCL,
                    S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);  
                if (index == -1)
                    return RTree<KeyT>{index,key_size,value_size,block_size,dimensions};
                if (ftruncate(index, block_size) == -1) {
                    close(index);
                    return RTree<KeyT>{index,key_size,value_size,block_size,dimensions};
                }

                RTree<KeyT> ret{index,key_size,value_size,block_size,dimensions};

                if (ret.get_fd() != -1) {
                    ret->get_header() = IndexHeader {
                        .Dimensions = dimensions,
                        .key_size = key_size,
                        .value_size = value_size,
                        .block_size = block_size,
                        .root_addr = INVALID_ROOT_ADDR,
                    }
                }

                return ret;
            }

            static RTree<KeyT> open(
                int dir,
                const char *name,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions) 
            {
                int index = openat(dir, name, O_RDWR);
                if (index == -1)
                    return RTree<KeyT>{index,key_size,value_size,block_size,dimensions};
                
                if (ret.get_fd() != -1) {
                    auto header = ret.get_header();

                    if (
                        header->key_size != key_size ||
                        header->value_size != value_size ||
                        header->block_size != block_size ||
                        header->Dimensions != dimensions
                    )
                        return RTree<KeyT>{index,key_size,value_size,block_size,dimensions};
                }

                return ret;
            }

        
        private:
            MMAPCache::FileCache    index_;
            uint64_t                key_size_;
            uint64_t                value_size_;
            uint64_t                block_size_;
            uint64_t                dimensions_;

            RTree(
                int index,
                uint64_t key_size,
                uint64_t value_size,
                uint64_t block_size,
                uint64_t dimensions
            ): index_{index,block_size},key_size_{key_size},value_size_{value_size},block_size_{block_size},dimensions_{dimensions} {
                assert(block_size>0 && block_size % PAGE_UNIT == 0);
            }
            
            template<typename T = void> T *get_address(uint64_t address) {
                auto ret = static_cast<T *>(index.get_block(address));

                if (ret == nullptr) {
                    abort();
                }

                return ret;
            }

            IndexHeader *get_header() { return get_address<IndexHeader>(0);}

            uint64_t get_root_addr() {
                auto index_header = get_header();
                return index_header->root_addr;
            }

            NodeHandler<RKeyType<KeyT>> get_node_handler(uint64_t address) {
                NodeHeader *header = get_address<NodeHeader>(address);

                return NodeHandler<KeyT>{header,key_size,value_size,block_size};
            }

            // Allocate one new block.
            uint64_t allocate_block(){
                uint64_t block = index.get_size();
                if (!index.truncate(block + get_block_size())) {
                    abort();
                }
                return block;
            }
            
            void search(
                const RKeyType<KeyT>& key,
                NodeHandler<RKeyType<KeyT>> *handler,
                std::vector<KeyValuePair<KeyT> *> res,
                SearchMode mode) 
            {
                auto entry_cnt = handler->get_count();
                    for(uint64_t i=0;i<entry_cnt;i++){
                        RKeyType<KeyT> *mbr = handler->get_elem_key();
                        if (handler->IsLeafBlock){
                            if (
                            (mode==SearchMode::overlap && *mbr.IsOverlap(key)) ||
                            (mode==SearchMode::comprise && *mbr>key)
                            ) {
                                res.push_back(handler->get_elem_pair(i));
                            }
                        }
                        else{
                            if (
                                (mode==SearchMode::overlap && *mbr.IsOverlap(key)) ||
                                (mode==SearchMode::comprise && *mbr>key)
                            ) {
                                auto next_addr = *reinterpret_cast<uint64_t *>(handler->get_elem_value(i))
                                auto next_handler = &get_node_handler(next_addr);
                                search(key,next_handler,res,mode);
                            }
                        }
                        
                    }
            }

            NodeHandler<RKeyType<KeyT>> *ChooseLeaf()

        public:
            void overlap_search(KeyT key){
                std::vector<KeyValuePair<KeyT>> res;
                auto root_handler = &get_node_handler(get_root_addr());
                return search(key,root_handler,res,SearchMode::overlap);
            }

            void comprise_search(KeyT key){
                std::vector<KeyValuePair<KeyT>> res;
                auto root_handler = &get_node_handler(get_root_addr());
                return search(key,root_handler,res,SearchMode::comprise);
            }
    };
}

#endif