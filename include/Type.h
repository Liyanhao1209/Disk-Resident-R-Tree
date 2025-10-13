#ifndef TYPE_H
#define TYPE_H

#include<vector>
#include<cassert>

namespace SpatialStorage {
    template <typename T>
    class KeyType {
    public:
        std::vector<T> data;

        KeyType() = default;
        KeyType(const std::vector<T>& initData) : data(initData) { assert(data.size()%2==0); }
        virtual ~KeyType() = default;
    };

    template <typename T>
    class RKeyType : public virtual KeyType<T> {
    public:
        size_t size() const {
            return this->data.size();
        }

        T enlargement(const RKeyType<T>& other) {
            assert(other.size()==this->size());
            auto size = this->size();
            T res = static_cast<T>(1);
            for(size_t i=0,j=size/2;i<size/2 && j<size;i++,j++){
                res *= max(this->data[j],other.data[j]) - min(this->data[i],other.data[i]);
            }

            return res;
        }

        T area() {
            auto size = this->size();
            T res = static_cast<T>(1);
            for(size_t i=0,j=size/2;i<size/2 && j<size;i++,j++){
                res *= this->data[j]-this->data[i];
            }

            return res;
        }

        void mbr_enlarge(const RKeyType<T>& other) {
            assert(other.size()==this->size());
            auto size = this->size();
            for(size_t i=0;i<size/2;i++){
                this->data[i] = min(this->data[i],other.data[i]);
            }
            for(size_t i=size/2;i<size;i++){
                this->data[i] = max(this->data[i],other.data[i]);
            }
        }

        bool operator>=(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();
            for (size_t i=0;i<size/2;i++){
                if (this->data[i]>other.data[i]) return false;
            }

            for (size_t i=size/2;i<size;i++){
                if (this->data[i]<other.data[i]) return false;
            }

            return true;
        }

        bool operator<=(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();
            for (size_t i=0;i<size/2;i++){
                if (this->data[i]<other.data[i]) return false;
            }

            for (size_t i=size/2;i<size;i++){
                if (this->data[i]>other.data[i]) return false;
            }

            return true;
        }

        bool operator==(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();
            for (size_t i=0;i<size/2;i++){
                if (this->data[i]!=other.data[i]) return false;
            }

            for (size_t i=size/2;i<size;i++){
                if (this->data[i]!=other.data[i]) return false;
            }

            return true;
        }

        bool operator>(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();
            for (size_t i=0;i<size/2;i++){
                if (this->data[i]>=other.data[i]) return false;
            }

            for (size_t i=size/2;i<size;i++){
                if (this->data[i]<=other.data[i]) return false;
            }

            return true;
        }

        bool operator<(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();
            for (size_t i=0;i<size/2;i++){
                if (this->data[i]<=other.data[i]) return false;
            }

            for (size_t i=size/2;i<size;i++){
                if (this->data[i]>=other.data[i]) return false;
            }

            return true;
        }

        bool IsOverlap(const RKeyType<T>& other) const {
            assert(other.size()==this->size());
            
            auto size = this->size();

            for(size_t i=0,j=size/2;i<size/2 && j<size;i++,j++){
                auto x1l = this->data[i];
                auto x1r = this->data[j];
                auto x2l = other.data[i];
                auto x2r = other.data[j];

                if (!(x1l<=x2r&&x1r>=x2l)) return false;
            }

            return true;
        }
    };

}

#endif