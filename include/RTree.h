#ifndef R_TREE_H
#define R_TREE_H

#include <cstdint>
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
            std::deque<std::pair<NodeHandler<KeyT>*,uint64_t>> path;
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

            int get_fd() { return index_.get_fd(); }

        
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

            size_t get_block_size() const { return block_size_; }

            IndexHeader *get_header() { return get_address<IndexHeader>(0);}

            uint64_t get_root_addr() {
                auto index_header = get_header();
                return index_header->root_addr;
            }

            NodeHandler<KeyT> get_node_handler(uint64_t address) {
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
                const KeyType<KeyT>& key,
                NodeHandler<KeyT> *handler,
                std::vector<KeyValuePair<KeyType<KeyT>> *> *res,
                SearchMode mode) 
            {
                auto entry_cnt = handler->get_count();
                    for(uint64_t i=0;i<entry_cnt;i++){
                        auto mbr = handler->get_elem_key(i);
                        if (handler->IsLeafBlock()){
                            if (
                            (mode==SearchMode::overlap && mbr.IsOverlap(key)) ||
                            (mode==SearchMode::comprise && mbr>=key)
                            ) {
                                KeyValuePair<KeyType<KeyT>> kvp = handler->get_elem_pair(i);
                                res->push_back(&kvp);
                            }
                        }
                        else{
                            if (
                                (mode==SearchMode::overlap && mbr.IsOverlap(key)) ||
                                (mode==SearchMode::comprise && mbr>=key)
                            ) {
                                auto next_addr = *reinterpret_cast<uint64_t *>(handler->get_elem_value(i));
                                auto next_handler = get_node_handler(next_addr);
                                search(key,&next_handler,res,mode);
                            }
                        }
                        
                    }
            }

            NodeHandler<KeyT> *ChooseLeaf(
                const KeyType<KeyT>& key,
                NodeHandler<KeyT>* handler,
                Context<KeyT>* ctx
            ) {
                

                if (handler->IsLeafBlock()) {
                    std::pair<NodeHandler<KeyT>*,uint64_t> pth(handler,0);
                    ctx->path.push_back(pth);
                    return handler;
                }

                KeyT *min_enlarge = nullptr;
                uint64_t min_enlarge_index = 0;

                auto entry_cnt = handler->get_count();
                for (uint64_t i=0;i<entry_cnt;i++){
                    auto mbr = handler->get_elem_key(i);
                    KeyT enlarge = mbr.enlargement(key);
                    if (min_enlarge==nullptr || enlarge < *min_enlarge) {
                        min_enlarge = &enlarge;
                        min_enlarge_index = i;
                    }
                }
                
                std::pair<NodeHandler<KeyT>*,uint64_t> pth(handler,min_enlarge_index);
                ctx->path.push_back(pth);
                auto next_addr = *reinterpret_cast<uint64_t *>(handler->get_elem_value(min_enlarge_index));
                auto next_handler = get_node_handler(next_addr);

                return ChooseLeaf(key,&next_handler,ctx);
            }

            NodeHandler<KeyT> *FindLeaf(
                Context<KeyT> *ctx,
                NodeHandler<KeyT> * cur_handler,
                KeyType<KeyT> *key
            ) {
                if (cur_handler->IsLeafBlock()) {
                    for(uint64_t i=0;i<cur_handler->get_count();i++){
                        if(*key==cur_handler->get_elem_key(i)){
                            std::pair<NodeHandler<KeyT>*,uint64_t> pth(cur_handler,i);
                            ctx->path.push_back(pth);
                            return cur_handler;
                        }
                    }
                    return nullptr;
                }

                for (uint64_t i=0;i<cur_handler->get_count();i++){
                    if(*key>=cur_handler->get_elem_key(i)){
                        uint64_t next_addr = *reinterpret_cast<uint64_t *>(cur_handler->get_elem_value(i));
                        NodeHandler<KeyT> next_node = get_node_handler(next_addr);
                        std::pair<NodeHandler<KeyT>*,uint64_t> pth(cur_handler,i);
                        ctx->path.push_back(pth);
                        auto res = FindLeaf(ctx,&next_node,key);
                        if(res==nullptr){
                            ctx->path.pop_back();
                        }
                        else{
                            return res;
                        }
                    }
                }

                return nullptr;
            }
            
            void modify_parent_entry_mbr(
                Context<KeyT> *ctx,
                KeyType<KeyT>* modify_key = nullptr
            ) {
                if (ctx->path.empty()){
                    return;
                }
                std::pair<NodeHandler<KeyT>*,uint64_t> handler_entry = ctx->path.back();
                NodeHandler<KeyT> *cur_handler = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                KeyType<KeyT> old_mbr = get_node_mbr(cur_handler);

                if(modify_key!=nullptr){
                    cur_handler->set_elem_key(modify_key,parent_entry_id);
                }
                KeyType<KeyT> new_modify_key = get_node_mbr(cur_handler);
                if(old_mbr!=new_modify_key){
                    modify_parent_entry_mbr(ctx,&new_modify_key);
                }
            }

            KeyType<KeyT> get_node_mbr(NodeHandler<KeyT>* handler) {
                auto first_key = handler->get_elem_key(0);
                KeyType<KeyT> mbr(first_key);

                for(uint64_t i=1;i<handler->get_count();i++){
                    mbr.mbr_enlarge(handler->get_elem_key(i));
                }

                return mbr;
            }

            void split(
                Context<KeyT> *ctx,
                KeyValuePair<KeyType<KeyT>>& insert_kvp,
                KeyType<KeyT>* modify_key = nullptr
            ) {
                std::pair<NodeHandler<KeyT>*,uint64_t> handler_entry = ctx->path.back();
                NodeHandler<KeyT> *cur_handler = handler_entry.first;
                uint64_t parent_entry_id = handler_entry.second;
                ctx->path.pop_back();

                if (modify_key!=nullptr){
                    cur_handler->set_elem_key(modify_key,parent_entry_id);
                }

                // not full,install(maybe modify)
                if(!cur_handler->is_full()){
                    cur_handler->insert(insert_kvp);
                    auto new_mbr = get_node_mbr(cur_handler);
                    if (modify_key == nullptr) {
                        modify_key = new KeyType<KeyT>(new_mbr); 
                    } else {
                        *modify_key = new_mbr;
                    }
                    
                    // propagate till root
                    modify_parent_entry_mbr(ctx,modify_key);
                    return;
                }

                // full,split
                auto new_addr = allocate_block();
                NodeHeader *new_header = get_address<NodeHeader>(new_addr);

                // set node header
                BlockType block_type = cur_handler->IsLeafBlock()?BlockType::LeafBlock:BlockType::InnerBlock;
                *new_header = NodeHeader{block_type,0,new_addr};
                auto seeds = pickseed(cur_handler, insert_kvp);
                auto& partition1 = seeds.first;  
                auto& partition2 = seeds.second;
                NodeHandler<KeyT> new_node_handler = get_node_handler(new_addr);

                // insert k/v pair and maintain the mbr
                KeyType<KeyT> mbr1(partition1[0]->key);
                for(uint64_t i =0;i<partition1.size();i++){
                    new_node_handler.insert(*partition1[i]);
                    mbr1.mbr_enlarge(partition1[i]->key);
                }

                // reinsert the original node and maintain the modified mbr
                cur_handler->clear();
                KeyType<KeyT> mbr2(partition2[0]->key);
                for(uint64_t i=0;i<partition2.size();i++) {
                    cur_handler->insert(*partition2[i]);
                    mbr2.mbr_enlarge(partition2[i]->key);
                }
                
                if (cur_handler->get_in_file_addr()==get_root_addr()){
                    uint64_t new_root_addr = allocate_block();
                    NodeHandler<KeyT> root_handler = get_node_handler(new_root_addr);
                    NodeHeader root_header = NodeHeader{BlockType::InnerBlock,0,new_root_addr};

                    root_handler.set_header(&root_header);
                    auto cur_in_file_addr = cur_handler->get_in_file_addr();
                    KeyValuePair<KeyType<KeyT>> kvp1{mbr2,&cur_in_file_addr};
                    KeyValuePair<KeyType<KeyT>> kvp2{mbr1,&new_addr};
                    root_handler.insert(kvp1);
                    root_handler.insert(kvp2);

                    get_header()->root_addr = new_root_addr;

                    return;
                }

                // traceback and adjust the tree
                // insert the new mbr entry and modify the parent mbr entry
                KeyValuePair<KeyType<KeyT>> kvp{mbr1,&new_addr};
                split(ctx,kvp,&mbr2);

            }

            std::pair<
                std::vector<KeyValuePair<KeyType<KeyT>> *>, 
                std::vector<KeyValuePair<KeyType<KeyT>> *> 
            >
                pickseed(NodeHandler<KeyT> *handler,KeyValuePair<KeyType<KeyT>>& kvp){
                    std::vector<KeyValuePair<KeyType<KeyT>> *> whole;
                    auto entry_count = handler->get_count();

                    for(uint64_t i = 0;i<entry_count;i++){
                        KeyValuePair<KeyType<KeyT>> kvp = handler->get_elem_pair(i);
                        whole.push_back(&kvp);
                    }

                    whole.push_back(&kvp);

                    size_t partition1;
                    size_t partition2;
                    KeyT min_waste;

                    for(size_t i=0;i<whole.size();i++){
                        for(size_t j=i+1;j<whole.size();j++){
                            KeyType<KeyT>& mbr1 = whole[i]->key;
                            KeyType<KeyT>& mbr2 = whole[j]->key;

                            KeyT waste = mbr1.enlargement(mbr2) - mbr1.area() - mbr2.area();
                            if (i==0&&j==1 || waste<min_waste){
                                min_waste = waste;
                                partition1 = i;
                                partition2 = j;
                            }
                        }
                    }

                    std::vector<KeyValuePair<KeyType<KeyT>> *> res1;
                    std::vector<KeyValuePair<KeyType<KeyT>> *> res2;

                    res1.push_back(whole[partition1]);
                    res2.push_back(whole[partition2]);

                    KeyType<KeyT> mbr1(res1[0]->key);
                    KeyType<KeyT> mbr2(res2[0]->key);

                    for(size_t i=0;i<whole.size();i++){
                        if (i==partition1 || i==partition2){
                            continue;
                        }

                        KeyType<KeyT> mbr = whole[i]->key;
                        KeyT area = mbr.area();
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

                    return std::make_pair(res1, res2);
                }


        public:
            std::vector<KeyValuePair<KeyType<KeyT>> *> Overlap_Search(const KeyType<KeyT>& key){
                assert(key.size()/2==this->dimensions_);
                std::vector<KeyValuePair<KeyType<KeyT>> *> res;
                auto root_handler = get_node_handler(get_root_addr());
                search(key,&root_handler,&res,SearchMode::overlap);

                return res;
            }

            std::vector<KeyValuePair<KeyType<KeyT>> *> Comprise_Search(const KeyType<KeyT>& key){
                assert(key.size()/2==this->dimensions_);
                std::vector<KeyValuePair<KeyType<KeyT>> *> res;
                auto root_handler = get_node_handler(get_root_addr());
                search(key,&root_handler,&res,SearchMode::comprise);

                return res;
            }

            void Insert(KeyValuePair<KeyType<KeyT>>& kvp) {
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

                    NodeHeader root_header = NodeHeader{BlockType::LeafBlock,0,new_addr};
                    NodeHeader *header = get_address<NodeHeader>(new_addr);
                    *header = root_header;
                    // root_handler.set_header(&root_header);

                    NodeHandler<KeyT> root_handler = get_node_handler(new_addr);
                    root_handler.insert(kvp);
                    return;
                }
                
                Context<KeyT> ctx;
                auto root_handler = get_node_handler(root_addr);
                ChooseLeaf(kvp.key,&root_handler,&ctx);
                
                split(&ctx,kvp);
            }


            bool Delete(KeyValuePair<KeyType<KeyT>>& kvp) {
                Context<KeyT> ctx;
                auto root_handler = get_node_handler(get_root_addr());
                NodeHandler<KeyT> *target_leaf = FindLeaf(&ctx,&root_handler,&kvp.key);

                if (target_leaf==nullptr){
                    return false;
                }

                std::pair<NodeHandler<KeyT>*,uint64_t> handler_entry = ctx.path.back();
                uint64_t parent_entry_id = handler_entry.second;
                target_leaf->delete_elem_key(parent_entry_id);

                modify_parent_entry_mbr(&ctx);

                return true;
            }

    };
}

#endif