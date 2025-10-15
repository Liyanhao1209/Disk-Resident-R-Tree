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
            std::deque<std::pair<NodeHandler<RKeyType<KeyT>>*,uint64_t>> path;
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
                    *ret.get_header() = IndexHeader {
                        .Dimensions = dimensions,
                        .key_size = key_size,
                        .value_size = value_size,
                        .block_size = block_size,
                        .root_addr = INVALID_ROOT_ADDR,
                    };
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
                
                RTree<KeyT> ret{index,key_size,value_size,block_size,dimensions};
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
                auto ret = static_cast<T *>(index_.get_block(address));

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

                return NodeHandler<KeyT>{header,key_size_,value_size_,block_size_};
            }

            // Allocate one new block.
            uint64_t allocate_block(){
                uint64_t block = index_.get_size();
                if (!index_.truncate(block + get_block_size())) {
                    abort();
                }
                return block;
            }
            
            void search(
                const RKeyType<KeyT>& key,
                NodeHandler<RKeyType<KeyT>> *handler,
                std::vector<KeyValuePair<KeyT> *> *res,
                SearchMode mode) 
            {
                auto entry_cnt = handler->get_count();
                    for(uint64_t i=0;i<entry_cnt;i++){
                        RKeyType<KeyT> *mbr = handler->get_elem_key(i);
                        if (handler->IsLeafBlock()){
                            if (
                            (mode==SearchMode::overlap && *mbr.IsOverlap(key)) ||
                            (mode==SearchMode::comprise && *mbr>key)
                            ) {
                                res->push_back(handler->get_elem_pair(i));
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

            NodeHandler<RKeyType<KeyT>> *ChooseLeaf(
                const RKeyType<KeyT>& key,
                NodeHandler<RKeyType<KeyT>>* handler,
                Context<KeyT>* ctx
            ) {
                

                if (handler->IsLeafBlock()) {
                    std::pair<NodeHandler<RKeyType<KeyT>>*,uint64_t> pth(handler,0);
                    ctx->path.push_back(pth);
                    return handler;
                }

                KeyT *min_enlarge = nullptr;
                uint64_t min_enlarge_index = 0;

                auto entry_cnt = handler->get_count();
                for (uint64_t i=0;i<entry_cnt;i++){
                    RKeyType<KeyT> *mbr = handler->get_elem_key(i);
                    KeyT enlarge = mbr->enlargement(key);
                    if (min_enlarge==nullptr || enlarge < *min_enlarge) {
                        min_enlarge = &enlarge;
                        min_enlarge_index = i;
                    }
                }
                
                std::pair<NodeHandler<RKeyType<KeyT>>*,uint64_t> pth(handler,min_enlarge_index);
                ctx->path.push_back(pth);
                auto next_addr = *reinterpret_cast<uint64_t *>(handler->get_elem_value(min_enlarge_index));
                auto next_handler = get_node_handler(next_addr);

                return ChooseLeaf(key,&next_handler,ctx);
            }
            
            void modify_parent_entry_mbr(
                Context<KeyT> *ctx,
                RKeyType<KeyT>* modify_key
            ) {
                if (ctx->path.empty()){
                    return;
                }
                std::pair<NodeHandler<RKeyType<KeyT>>*,uint64_t> handler_entry = ctx->path.back();
                NodeHandler<RKeyType<KeyT>> *cur_handler = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                RKeyType<KeyT> *new_modify_key = get_node_mbr(cur_handler);

                modify_parent_entry_mbr(ctx,new_modify_key);
            }

            RKeyType<KeyT> *get_node_mbr(NodeHandler<RKeyType<KeyT>>* handler) {
                RKeyType<KeyT> *first_key = cur_handler->get_elem_key(0);
                RKeyType<KeyT> mbr(first_key->data);

                for(uint64_t i=1;i<cur_handler->get_count();i++){
                    mbr.mbr_enlarge(*cur_handler->get_elem_key(i));
                }

                return &mbr;
            }

            void split(
                Context<KeyT> *ctx,
                KeyValuePair<RKeyType<KeyT>>& insert_kvp,
                RKeyType<KeyT>* modify_key = nullptr
            ) {
                std::pair<NodeHandler<RKeyType<KeyT>>*,uint64_t> handler_entry = ctx->path.back();
                NodeHandler<RKeyType<KeyT>> *cur_handler = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                // not full,install(maybe modify)
                if(!cur_handler->is_full()){
                    cur_handler->insert(kvp);
                    if (modify_key!=nullptr){
                        cur_handler->set_elem_key(modify_key,parent_entry_id);
                    }
                    modify_key = get_node_mbr(cur_handler); 
                    
                    // propagate till root
                    modify_parent_entry_mbr(ctx,modify_key);
                }

                // full,split
                auto new_addr = allocate_block();
                NodeHeader *new_header = get_address<NodeHeader>(new_addr);

                // set node header
                BlockType block_type = cur_handler->IsLeafBlock()?BlockType::LeafBlock:BlockType::InnerBlock;
                *new_header = NodeHeader{block_type,0,new_addr};
                std::pair<
                    std::vector<KeyValuePair<RKeyType<KeyT>> *>, 
                    std::vector<KeyValuePair<RKeyType<KeyT>> *> 
                > seed = pickseed(cur_handler,insert_kvp);
                std::vector<KeyValuePair<RKeyType<KeyT>> *> partition1 = seed.first;
                std::vector<KeyValuePair<RKeyType<KeyT>> *> partition2 = seed.second;
                NodeHandler new_node_handler = get_node_handler(new_addr);

                // insert k/v pair and maintain the mbr
                RKeyType<KeyT> mbr1(*partition1[0]);
                for(uint64_t i =0;i<partition1.size();i++){
                    new_node_handler.insert(*partition1[i]);
                    mbr1.mbr_enlarge(*partition1[i].key)
                }

                // reinsert the original node and maintain the modified mbr
                cur_handler->clear();
                RKeyType<KeyT> mbr2(*partition2[0]);
                for(uint64_t i=0;i<partiton2.size();i++) {
                    cur_handler->insert(*partition2[i]);
                    mbr2.mbr_enlarge(*partition2[i].key);
                }
                
                // traceback and adjust the tree
                // insert the new mbr entry and modify the parent mbr entry
                split(ctx,KeyValuePair{mbr1,&new_addr},&mbr2);

            }

            std::pair<
                std::vector<KeyValuePair<RKeyType<KeyT>> *>, 
                std::vector<KeyValuePair<RKeyType<KeyT>> *> 
            >
                pickseed(NodeHandler<RKeyType<KeyT>> *handler,KeyValuePair<RKeyType<KeyT>>& kvp){
                    std::vector<KeyValuePair<RKeyType<KeyT>> *> whole;
                    auto entry_count = handler->get_count();

                    for(uint64_t i = 0;i<entry_count;i++){
                        whole.push_back(handler->get_elem_pair(i));
                    }

                    whole.push_back(kvp);

                    size_t partition1;
                    size_t partition2;
                    KeyT min_waste;

                    for(size_t i=0;i<whole.size();i++){
                        for(size_t j=i+1;j<whole.size();j++){
                            RKeyType<KeyT> *mbr1 = whole[i]->key;
                            RKeyType<KeyT> *mbr2 = whole[j]->key;

                            KeyT waste = mbr1->enlargement(mbr2) - mbr1->area() - mbr2->area();
                            if (i==0&&j==1 || waste<min_waste){
                                min_waste = waste;
                                partition1 = i;
                                partition2 = j;
                            }
                        }
                    }

                    std::vector<KeyValuePair<RKeyType<KeyT>> *> res1;
                    std::vector<KeyValuePair<RKeyType<KeyT>> *> res2;

                    res1.push_back(whole[partition1]);
                    res2.push_back(whole[partition2]);

                    RKeyType<KeyT> mbr1(&res1[0]->key.data);
                    RKeyType<KeyT> mbr2(&res2[0]->key.data);

                    for(size_t i=0;i<whole.size();i++){
                        if (i==partition1 || i==partition2){
                            continue;
                        }

                        RKeyType<KeyT> *mbr = whole[i]->key;
                        KeyT area = mbr->area();
                        KeyT waste1 = mbr1.enlargement(mbr) - area - mbr1.area();
                        KeyT waste2 = mbr2.enlargement(mbr) - area - mbr2.area();
                        
                        if (waste1<waste2){
                            res1.push_back(whole[i]);
                            mbr1.mbr_enlarge(mbr);
                        }else {
                            res2.push_back(whole[i]);
                            mbr2.mbr_enlarge(mbr);
                        }

                    }

                    return std::pair<
                        std::vector<KeyValuePair<RKeyType<KeyT>> *>, 
                        std::vector<KeyValuePair<RKeyType<KeyT>> *>
                    > res(&res1,&res2);
                }


        public:
            std::vector<KeyValuePair<KeyT>> *overlap_search(const RKeyType<KeyT>& key){
                assert(key.size()==this->dimensions_);
                std::vector<KeyValuePair<KeyT> *> res;
                auto root_handler = &get_node_handler(get_root_addr());
                search(&key,root_handler,&res,SearchMode::overlap);

                return &res;
            }

            std::vector<KeyValuePair<KeyT>> *comprise_search(const RKeyType<KeyT>& key){
                assert(key.size()==this->dimensions_);
                std::vector<KeyValuePair<KeyT> *> res;
                auto root_handler = &get_node_handler(get_root_addr());
                search(key,root_handler,res,SearchMode::comprise);

                return &res;
            }

            void insert(KeyValuePair<RKeyType<KeyT>>& kvp) {
                auto root_addr = get_root_addr();
                // empty tree
                if (root_addr == INVALID_ROOT_ADDR) {
                    /**
                     * allocate a new lbock
                     * set the IndexHeader's root_addr to the new addr
                     * init a Node Header,set its metadata
                     * set the node header
                     * insert the k/v pair
                     */
                    uint64_t new_addr = allocate_block();
                    get_header()->root_addr = new_addr;

                    NodeHandler<RKeyType<KeyT>> *root_handler = &get_node_handler(new_addr);
                    NodeHeader root_header = NodeHeader{BlockType::LeafBlock,0,new_addr};
                    root_handler->set_header(&root_header);

                    root_handler->insert(kvp);
                }
                
                Context<KeyT> ctx;
                auto root_handler = get_node_handler(root_addr);
                ChooseLeaf(kvp.key,&root_handler,&ctx);
                
                split(ctx,kvp);
            }


    };
}

#endif