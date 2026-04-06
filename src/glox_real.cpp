#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "Ws2_32.lib")
  using socklen_t = int;
#else
  #include <arpa/inet.h>
  #include <netdb.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>
  static inline int closesocket(int s){ return ::close(s); }
#endif

#include <emp-tool/emp-tool.h>
#include <emp-sh2pc/emp-sh2pc.h>
#include <emp-tool/circuits/circuit_file.h>

using emp::block;
using emp::PRG;
using emp::NetIO;
using emp::Bit;

static void die(const std::string& m){ std::cerr << "[fatal] " << m << "\n"; std::exit(1); }

// ---------- sockets ----------
static std::pair<std::string,int> parse_hostport(const std::string& s){
  auto p = s.find(":");
  if (p==std::string::npos) die("bad host:port: "+s);
  return {s.substr(0,p), std::stoi(s.substr(p+1))};
}
static int dial(const std::string& host, int port){
#ifdef _WIN32
  WSADATA wsa; WSAStartup(MAKEWORD(2,2), &wsa);
#endif
  addrinfo hints{}; hints.ai_family = AF_UNSPEC; hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res)!=0) die("getaddrinfo failed");
  int fd=-1;
  for (auto *rp=res; rp; rp=rp->ai_next){
    fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (fd==-1) continue;
    if (connect(fd, rp->ai_addr, (int)rp->ai_addrlen)==0) break;
    closesocket(fd); fd=-1;
  }
  freeaddrinfo(res);
  if (fd==-1) die("connect failed");
  return fd;
}
static int listen_on(int port){
#ifdef _WIN32
  WSADATA wsa; WSAStartup(MAKEWORD(2,2), &wsa);
#endif
  int fd = socket(AF_INET, SOCK_STREAM, 0); if(fd<0) die("socket");
  int opt=1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));
  sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_addr.s_addr=INADDR_ANY; addr.sin_port=htons((uint16_t)port);
  if(bind(fd, (sockaddr*)&addr, sizeof(addr))<0) die("bind");
  if(listen(fd, 16)<0) die("listen");
  return fd;
}
static void send_all(int fd, const uint8_t* p, size_t n){
  size_t off=0;
  while(off<n){
    int k = ::send(fd, (const char*)p+off, (int)std::min(n-off, (size_t)1<<20), 0);
    if(k<=0) die("send");
    off += (size_t)k;
  }
}
static void recv_all(int fd, uint8_t* p, size_t n){
  size_t off=0;
  while(off<n){
    int k = ::recv(fd, (char*)p+off, (int)std::min(n-off, (size_t)1<<20), 0);
    if(k<=0) die("recv");
    off += (size_t)k;
  }
}

// ---------- RSS/HWM (Linux/WSL) ----------
static bool read_proc_status_kb(long& rss_kb, long& hwm_kb){
#ifndef _WIN32
  std::ifstream f("/proc/self/status");
  if(!f) return false;
  std::string line;
  rss_kb = -1; hwm_kb = -1;
  while(std::getline(f,line)){
    if(line.rfind("VmRSS:",0)==0){
      std::sscanf(line.c_str(), "VmRSS: %ld kB", &rss_kb);
    } else if(line.rfind("VmHWM:",0)==0){
      std::sscanf(line.c_str(), "VmHWM: %ld kB", &hwm_kb);
    }
  }
  return (rss_kb>=0 && hwm_kb>=0);
#else
  rss_kb = hwm_kb = -1;
  return false;
#endif
}
static double kb_to_mb(long kb){ return (kb<0)?0.0: (double)kb/1024.0; }

// ---------- helpers ----------
static inline uint8_t get_bit_u64(uint64_t x, size_t i){ return (uint8_t)((x >> i) & 1ULL); }
static inline bool is_pow2(size_t x){ return x && ((x & (x-1))==0); }
static inline block xor_block(const block& a, const block& b){
  block r;
  emp::xorBlocks_arr(&r, &a, &b, 1);
  return r;
}

static uint16_t load_u16_le(const uint8_t* p){
  return (uint16_t)p[0] | ((uint16_t)p[1] << 8);
}
static uint32_t load_u32_le(const uint8_t* p){
  return (uint32_t)p[0]
       | ((uint32_t)p[1] << 8)
       | ((uint32_t)p[2] << 16)
       | ((uint32_t)p[3] << 24);
}
static void store_u16_le(uint8_t* p, uint16_t v){
  p[0] = (uint8_t)(v & 0xffu);
  p[1] = (uint8_t)((v >> 8) & 0xffu);
}
static void store_u32_le(uint8_t* p, uint32_t v){
  p[0] = (uint8_t)(v & 0xffu);
  p[1] = (uint8_t)((v >> 8) & 0xffu);
  p[2] = (uint8_t)((v >> 16) & 0xffu);
  p[3] = (uint8_t)((v >> 24) & 0xffu);
}

static uint64_t splitmix64(uint64_t x){
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

static std::vector<uint8_t> deterministic_bytes(uint64_t domain_tag, uint64_t idx, size_t n){
  std::vector<uint8_t> out(n, 0);
  uint64_t state = splitmix64(domain_tag ^ (idx * 0x9e3779b97f4a7c15ULL));
  for(size_t off=0; off<n; off+=8){
    state = splitmix64(state + off + 0x517cc1b727220a95ULL);
    for(size_t j=0; j<8 && (off+j)<n; j++){
      out[off+j] = (uint8_t)((state >> (8*j)) & 0xffu);
    }
  }
  return out;
}

static uint64_t io_bytes(NetIO* io) {
  return (uint64_t)io->counter;
}

// ============================================================
// Op tags (1 byte per query)
// ============================================================
enum OpTag : uint8_t {
  OP_GB = 1,
  OP_RB_BEST = 2,
  OP_RB_WORST = 3,
  OP_DIR = 4
};

static const char* optag_name(uint8_t t){
  switch(t){
    case OP_GB: return "GB";
    case OP_RB_BEST: return "RB_BEST";
    case OP_RB_WORST: return "RB_WORST";
    case OP_DIR: return "DIR";
    default: return "UNK";
  }
}

constexpr size_t QUERY_PAYLOAD_BYTES = 16;
constexpr size_t STATE_RECORD_STRUCT_BYTES = 24;
constexpr uint8_t STATE_FLAG_BLOCKED = 0x01;
constexpr uint8_t STATE_FLAG_SPARE_VALID = 0x02;
constexpr uint8_t STATE_FLAG_CAN_PROMOTE = 0x04;

struct PlainStateRecord {
  uint32_t bucket_id = 0;
  uint32_t spare_bucket = 0;
  uint32_t dir_index = 0;
  uint32_t token_nonce = 0;
  uint16_t token_epoch = 0;
  uint8_t level = 0;
  uint8_t invites_remaining = 0;
  uint8_t blockages = 0;
  uint8_t flags = 0;
};

struct QueryPayload {
  uint32_t token_nonce = 0;
  uint32_t requested_dir_index = 0;
  uint16_t token_epoch = 0;
  uint16_t flags = 0;
  uint32_t client_tag = 0;
};

struct CircuitPublicResult {
  bool allow = false;
  uint8_t next_level = 0;
  uint8_t next_blockages = 0;
  uint8_t next_invites = 0;
  bool next_blocked = false;
  uint32_t selected_bucket = 0;
  uint32_t selected_dir_index = 0;
  uint32_t next_token_nonce = 0;
  uint64_t checksum = 0;
};

static std::array<uint8_t, QUERY_PAYLOAD_BYTES> encode_query_payload(const QueryPayload& p){
  std::array<uint8_t, QUERY_PAYLOAD_BYTES> out{};
  store_u32_le(out.data() + 0, p.token_nonce);
  store_u32_le(out.data() + 4, p.requested_dir_index);
  store_u16_le(out.data() + 8, p.token_epoch);
  store_u16_le(out.data() + 10, p.flags);
  store_u32_le(out.data() + 12, p.client_tag);
  return out;
}

static QueryPayload decode_query_payload(const std::array<uint8_t, QUERY_PAYLOAD_BYTES>& in){
  QueryPayload p;
  p.token_nonce = load_u32_le(in.data() + 0);
  p.requested_dir_index = load_u32_le(in.data() + 4);
  p.token_epoch = load_u16_le(in.data() + 8);
  p.flags = load_u16_le(in.data() + 10);
  p.client_tag = load_u32_le(in.data() + 12);
  return p;
}

static PlainStateRecord decode_state_record_prefix(const uint8_t* p, size_t n){
  if(n < STATE_RECORD_STRUCT_BYTES) die("state record buffer too small");
  PlainStateRecord r;
  r.bucket_id = load_u32_le(p + 0);
  r.spare_bucket = load_u32_le(p + 4);
  r.dir_index = load_u32_le(p + 8);
  r.token_nonce = load_u32_le(p + 12);
  r.token_epoch = load_u16_le(p + 16);
  r.level = p[18];
  r.invites_remaining = p[19];
  r.blockages = p[20];
  r.flags = p[21];
  return r;
}

static std::vector<uint8_t> encode_state_record_bytes(const PlainStateRecord& r, size_t B){
  if(B < STATE_RECORD_STRUCT_BYTES) die("B must be at least STATE_RECORD_STRUCT_BYTES");
  std::vector<uint8_t> out(B, 0);
  store_u32_le(out.data() + 0, r.bucket_id);
  store_u32_le(out.data() + 4, r.spare_bucket);
  store_u32_le(out.data() + 8, r.dir_index);
  store_u32_le(out.data() + 12, r.token_nonce);
  store_u16_le(out.data() + 16, r.token_epoch);
  out[18] = r.level;
  out[19] = r.invites_remaining;
  out[20] = r.blockages;
  out[21] = r.flags;
  return out;
}

static PlainStateRecord make_plain_state_record(size_t idx, size_t M, size_t N){
  PlainStateRecord r;
  r.bucket_id = (uint32_t)idx;
  r.spare_bucket = (uint32_t)((idx + std::max<size_t>(1, M/2)) % M);
  r.dir_index = (uint32_t)((idx * 17 + 11) % N);
  r.token_nonce = (uint32_t)(0x9e3779b9u * (uint32_t)(idx + 1) + 0x7f4a7c15u);
  r.token_epoch = (uint16_t)(100u + (idx % 29u));
  r.level = (uint8_t)(idx % 5u);
  r.invites_remaining = (uint8_t)(1u + (idx % 4u));
  r.blockages = (uint8_t)(idx % 3u);
  r.flags = STATE_FLAG_CAN_PROMOTE;
  if(((idx / 7u) % 8u) == 0u) r.flags |= STATE_FLAG_BLOCKED;
  if(((idx / 5u) % 4u) != 0u) r.flags |= STATE_FLAG_SPARE_VALID;
  return r;
}

static std::vector<uint8_t> make_state_record_bytes(size_t idx, size_t M, size_t N, size_t B){
  return encode_state_record_bytes(make_plain_state_record(idx, M, N), B);
}

static std::vector<uint8_t> make_dir_record_bytes(size_t idx, size_t D){
  std::vector<uint8_t> actual(D, 0);
  for(size_t i=0;i<D;i++){
    uint64_t mix = splitmix64((uint64_t)idx * 0xd1342543de82ef95ULL + i);
    actual[i] = (uint8_t)(mix & 0xffu);
  }
  if(D >= 8){
    store_u32_le(actual.data() + 0, (uint32_t)idx);
    store_u32_le(actual.data() + 4, (uint32_t)(idx ^ 0xa5a5a5a5u));
  }
  return actual;
}

static std::pair<std::array<uint8_t, QUERY_PAYLOAD_BYTES>, std::array<uint8_t, QUERY_PAYLOAD_BYTES>>
split_query_payload(const QueryPayload& payload){
  auto actual = encode_query_payload(payload);
  auto rnd = deterministic_bytes(0x5041594c4f41445fULL, payload.client_tag, QUERY_PAYLOAD_BYTES);
  std::array<uint8_t, QUERY_PAYLOAD_BYTES> p0{}, p1{};
  for(size_t i=0;i<QUERY_PAYLOAD_BYTES;i++){
    p0[i] = rnd[i];
    p1[i] = (uint8_t)(actual[i] ^ rnd[i]);
  }
  return {p0, p1};
}

// ============================================================
// Real DPF (EUROCRYPT'15-style for point functions) - compact tree variant
// ============================================================

struct DPFKey {
  block seed;
  uint8_t t;
  std::vector<block> cw_seed;
  std::vector<uint8_t> cw_tl;
  std::vector<uint8_t> cw_tr;
  uint8_t cw_out;
};

struct ExpandOut {
  block sL, sR;
  uint8_t tL, tR;
};

static ExpandOut prg_expand(const block& s){
  PRG prg(&s);
  std::array<block,2> out;
  prg.random_block(out.data(), 2);
  uint64_t w=0;
  prg.random_data(&w, sizeof(w));
  ExpandOut e;
  e.sL = out[0];
  e.sR = out[1];
  e.tL = (uint8_t)(w & 1ULL);
  e.tR = (uint8_t)((w >> 1) & 1ULL);
  return e;
}

static std::pair<DPFKey,DPFKey> DPF_Gen(uint64_t a, size_t N){
  if(!is_pow2(N)) die("DPF_Gen: N must be power-of-two");
  size_t n = (size_t)std::log2((double)N);

  PRG prg;
  block s0, s1;
  prg.random_block(&s0, 1);
  prg.random_block(&s1, 1);

  DPFKey k0, k1;
  k0.seed=s0; k1.seed=s1;
  k0.t=0; k1.t=1;

  k0.cw_seed.resize(n);
  k1.cw_seed.resize(n);
  k0.cw_tl.resize(n); k0.cw_tr.resize(n);
  k1.cw_tl.resize(n); k1.cw_tr.resize(n);

  block s0_i=s0, s1_i=s1;
  uint8_t t0_i=0, t1_i=1;

  for(size_t level=0; level<n; level++){
    ExpandOut e0 = prg_expand(s0_i);
    ExpandOut e1 = prg_expand(s1_i);

    size_t bitpos = n-1-level;
    uint8_t a_bit = get_bit_u64(a, bitpos);

    block s0_keep = (a_bit==0)? e0.sL : e0.sR;
    block s1_keep = (a_bit==0)? e1.sL : e1.sR;
    block cwS = xor_block(s0_keep, s1_keep);

    uint8_t cw_tL = (uint8_t)(e0.tL ^ e1.tL ^ (a_bit==0));
    uint8_t cw_tR = (uint8_t)(e0.tR ^ e1.tR ^ (a_bit==1));

    k0.cw_seed[level]=cwS; k1.cw_seed[level]=cwS;
    k0.cw_tl[level]=cw_tL; k1.cw_tl[level]=cw_tL;
    k0.cw_tr[level]=cw_tR; k1.cw_tr[level]=cw_tR;

    auto next_update = [&](block /*s_i*/, uint8_t t_i, const ExpandOut& e)->std::pair<block,uint8_t>{
      block s_next = (a_bit==0)? e.sL : e.sR;
      uint8_t t_next = (a_bit==0)? e.tL : e.tR;
      if(t_i){
        s_next = xor_block(s_next, cwS);
        t_next = (uint8_t)(t_next ^ ((a_bit==0)? cw_tL : cw_tR));
      }
      return {s_next, t_next};
    };

    auto u0 = next_update(s0_i, t0_i, e0);
    auto u1 = next_update(s1_i, t1_i, e1);
    s0_i=u0.first; t0_i=u0.second;
    s1_i=u1.first; t1_i=u1.second;
  }

  auto leaf_bit = [&](const block& s)->uint8_t{
    PRG prg(&s);
    uint8_t b=0;
    prg.random_data(&b, 1);
    return (uint8_t)(b & 1);
  };
  uint8_t out0 = leaf_bit(s0_i);
  uint8_t out1 = leaf_bit(s1_i);
  uint8_t cw_out = (uint8_t)(out0 ^ out1 ^ 1);
  k0.cw_out = cw_out;
  k1.cw_out = cw_out;

  return {k0,k1};
}

static uint8_t DPF_Eval(const DPFKey& k, uint64_t x, size_t N){
  if(!is_pow2(N)) die("DPF_Eval: N must be power-of-two");
  size_t n = (size_t)std::log2((double)N);

  block s = k.seed;
  uint8_t t = k.t;

  for(size_t level=0; level<n; level++){
    ExpandOut e = prg_expand(s);
    size_t bitpos = n-1-level;
    uint8_t x_bit = get_bit_u64(x, bitpos);

    block sL = e.sL, sR = e.sR;
    uint8_t tL = e.tL, tR = e.tR;

    if(t){
      if(x_bit==0){
        sL = xor_block(sL, k.cw_seed[level]);
        tL = (uint8_t)(tL ^ k.cw_tl[level]);
      } else {
        sR = xor_block(sR, k.cw_seed[level]);
        tR = (uint8_t)(tR ^ k.cw_tr[level]);
      }
    }

    s = (x_bit==0)? sL : sR;
    t = (x_bit==0)? tL : tR;
  }

  PRG prg(&s);
  uint8_t b=0;
  prg.random_data(&b, 1);
  uint8_t out = (uint8_t)(b & 1);
  if(t) out = (uint8_t)(out ^ k.cw_out);
  return out;
}

static void write_block(std::vector<uint8_t>& buf, const block& b){
  size_t old=buf.size(); buf.resize(old+sizeof(block));
  std::memcpy(buf.data()+old, &b, sizeof(block));
}
static block read_block(const uint8_t* p){
  block b; std::memcpy(&b, p, sizeof(block)); return b;
}
static std::vector<uint8_t> DPF_Serialize(const DPFKey& k){
  std::vector<uint8_t> out;
  auto push8=[&](uint8_t v){ out.push_back(v); };
  write_block(out, k.seed);
  push8(k.t);
  uint32_t n = (uint32_t)k.cw_seed.size();
  out.insert(out.end(), (uint8_t*)&n, (uint8_t*)&n + sizeof(n));
  for(size_t i=0;i<k.cw_seed.size();i++){
    write_block(out, k.cw_seed[i]);
    push8(k.cw_tl[i]);
    push8(k.cw_tr[i]);
  }
  push8(k.cw_out);
  return out;
}
static DPFKey DPF_Deserialize(const std::vector<uint8_t>& in){
  DPFKey k;
  size_t off=0;
  k.seed = read_block(in.data()+off); off += sizeof(block);
  k.t = in[off++];
  uint32_t n=0; std::memcpy(&n, in.data()+off, sizeof(n)); off += sizeof(n);
  k.cw_seed.resize(n);
  k.cw_tl.resize(n);
  k.cw_tr.resize(n);
  for(uint32_t i=0;i<n;i++){
    k.cw_seed[i] = read_block(in.data()+off); off += sizeof(block);
    k.cw_tl[i] = in[off++];
    k.cw_tr[i] = in[off++];
  }
  k.cw_out = in[off++];
  return k;
}

template<typename T>
static emp::Integer xor_shared_integer(int bits, T local_share, int party){
  T mine = local_share;
  T zero = 0;
  emp::Integer a(bits, (party == emp::ALICE) ? &mine : &zero, emp::ALICE);
  emp::Integer b(bits, (party == emp::BOB) ? &mine : &zero, emp::BOB);
  return a ^ b;
}

static CircuitPublicResult run_state_circuit_real(
  int party,
  NetIO* io,
  uint8_t op,
  const PlainStateRecord& local_share,
  const QueryPayload& local_payload
){
  emp::setup_semi_honest(io, party);

  emp::Integer bucket = xor_shared_integer(32, local_share.bucket_id, party);
  emp::Integer spare_bucket = xor_shared_integer(32, local_share.spare_bucket, party);
  emp::Integer dir_index = xor_shared_integer(32, local_share.dir_index, party);
  emp::Integer token_nonce = xor_shared_integer(32, local_share.token_nonce, party);
  emp::Integer token_epoch = xor_shared_integer(16, local_share.token_epoch, party);
  emp::Integer level = xor_shared_integer(8, local_share.level, party);
  emp::Integer invites = xor_shared_integer(8, local_share.invites_remaining, party);
  emp::Integer blockages = xor_shared_integer(8, local_share.blockages, party);
  emp::Bit blocked = xor_shared_integer(1, (uint8_t)((local_share.flags & STATE_FLAG_BLOCKED) ? 1u : 0u), party)[0];
  emp::Bit spare_valid = xor_shared_integer(1, (uint8_t)((local_share.flags & STATE_FLAG_SPARE_VALID) ? 1u : 0u), party)[0];
  emp::Bit can_promote_flag = xor_shared_integer(1, (uint8_t)((local_share.flags & STATE_FLAG_CAN_PROMOTE) ? 1u : 0u), party)[0];

  emp::Integer q_token_nonce = xor_shared_integer(32, local_payload.token_nonce, party);
  emp::Integer q_requested_dir = xor_shared_integer(32, local_payload.requested_dir_index, party);
  emp::Integer q_token_epoch = xor_shared_integer(16, local_payload.token_epoch, party);

  const emp::Integer z8(8, 0, emp::PUBLIC);
  const emp::Integer one8(8, 1, emp::PUBLIC);
  const emp::Integer three8(8, 3, emp::PUBLIC);
  const emp::Integer four8(8, 4, emp::PUBLIC);
  const emp::Integer one32(32, 1, emp::PUBLIC);
  const emp::Integer promote_limit(8, 1, emp::PUBLIC);

  emp::Bit has_invites = invites > z8;
  emp::Bit selected_is_spare = blocked & spare_valid;

  emp::Bit allow = false;
  emp::Integer selected_bucket = bucket;
  emp::Integer selected_dir_index = dir_index;
  emp::Integer next_level = level;
  emp::Integer next_blockages = blockages;
  emp::Integer next_invites = invites;
  emp::Bit next_blocked = blocked;
  emp::Integer next_token_nonce = token_nonce;

  if(op == OP_GB){
    emp::Bit can_promote = can_promote_flag & !blocked & has_invites & (level < four8) & (blockages <= promote_limit);
    allow = !blocked;
    selected_bucket = emp::If(selected_is_spare, spare_bucket, bucket);
    next_level = emp::If(can_promote, level + one8, level);
    next_token_nonce = emp::If(allow & has_invites, token_nonce + one32, token_nonce);
  } else if(op == OP_RB_BEST || op == OP_RB_WORST){
    next_blockages = blockages + one8;
    emp::Bit over_threshold = next_blockages >= three8;
    emp::Bit can_migrate = (op == OP_RB_BEST) ? spare_valid : emp::Bit(false, emp::PUBLIC);
    emp::Bit should_migrate = over_threshold & can_migrate;
    emp::Bit no_spare_blocked = over_threshold & !can_migrate;
    emp::Bit can_drop_level = level > z8;
    allow = should_migrate;
    selected_bucket = emp::If(should_migrate, spare_bucket, bucket);
    next_level = emp::If(should_migrate & can_drop_level, level - one8, level);
    next_blocked = blocked | no_spare_blocked;
    next_token_nonce = token_nonce + one32;
  } else if(op == OP_DIR){
    emp::Bit token_matches = (token_nonce == q_token_nonce) & (token_epoch == q_token_epoch);
    emp::Bit dir_matches = (dir_index == q_requested_dir);
    allow = (!blocked) & has_invites & token_matches & dir_matches;
    next_invites = emp::If(allow, invites - one8, invites);
    next_token_nonce = emp::If(allow, token_nonce + one32, token_nonce);
    selected_bucket = bucket;
    selected_dir_index = dir_index;
  }

  emp::Integer checksum(64, 0, emp::PUBLIC);
  checksum = checksum ^ selected_bucket.resize(64, false);
  checksum = checksum ^ (selected_dir_index.resize(64, false) << 11);
  checksum = checksum ^ (next_token_nonce.resize(64, false) << 23);
  checksum = checksum ^ (next_level.resize(64, false) << 55);
  checksum = checksum ^ (next_blockages.resize(64, false) << 47);
  checksum = checksum ^ (next_invites.resize(64, false) << 39);
  checksum = checksum ^ (emp::If(allow, emp::Integer(1, 1, emp::PUBLIC), emp::Integer(1, 0, emp::PUBLIC)).resize(64, false) << 63);

  CircuitPublicResult out;
  out.allow = allow.reveal<bool>(emp::PUBLIC);
  out.next_level = (uint8_t)next_level.reveal<int>(emp::PUBLIC);
  out.next_blockages = (uint8_t)next_blockages.reveal<int>(emp::PUBLIC);
  out.next_invites = (uint8_t)next_invites.reveal<int>(emp::PUBLIC);
  out.next_blocked = next_blocked.reveal<bool>(emp::PUBLIC);
  out.selected_bucket = selected_bucket.reveal<uint32_t>(emp::PUBLIC);
  out.selected_dir_index = selected_dir_index.reveal<uint32_t>(emp::PUBLIC);
  out.next_token_nonce = next_token_nonce.reveal<uint32_t>(emp::PUBLIC);
  out.checksum = checksum.reveal<uint64_t>(emp::PUBLIC);

  emp::finalize_semi_honest();
  return out;
}

// ============================================================
// Servers
// ============================================================
struct ServerConfig {
  int port_client = 9001;
  std::string peer_host = "127.0.0.1";
  int port_peer = 9901;
  int party = emp::ALICE;  // 1 or 2
  bool is_dir = false;

  size_t M = 262144;
  size_t B = 128;
  size_t N = 65536;
  size_t D = 256;
  std::string server_csv;
};

static void append_server_csv(
  const ServerConfig& cfg,
  uint32_t Q,
  double gc_ms_total,
  uint64_t gc_bytes_total,
  const std::array<uint64_t, 5>& op_counts,
  const std::array<uint64_t, 5>& op_gc_bytes,
  const std::array<double, 5>& op_gc_ms,
  uint64_t allow_count,
  uint64_t checksum_xor,
  long rss_peak,
  long hwm_peak
){
  if(cfg.server_csv.empty()) return;
  std::ofstream f(cfg.server_csv, std::ios::app);
  if(f.tellp() == 0){
    f << "party,is_dir,Q,gc_ms_total,gc_bytes_total,allow_count,checksum_xor,"
         "gb_count,rb_best_count,rb_worst_count,dir_count,"
         "gb_gc_bytes,rb_best_gc_bytes,rb_worst_gc_bytes,dir_gc_bytes,"
         "gb_gc_ms,rb_best_gc_ms,rb_worst_gc_ms,dir_gc_ms,"
         "rss_mb,hwm_mb\n";
  }
  f << cfg.party << "," << (cfg.is_dir ? 1 : 0) << "," << Q << ","
    << gc_ms_total << "," << gc_bytes_total << "," << allow_count << "," << checksum_xor << ","
    << op_counts[OP_GB] << "," << op_counts[OP_RB_BEST] << "," << op_counts[OP_RB_WORST] << "," << op_counts[OP_DIR] << ","
    << op_gc_bytes[OP_GB] << "," << op_gc_bytes[OP_RB_BEST] << "," << op_gc_bytes[OP_RB_WORST] << "," << op_gc_bytes[OP_DIR] << ","
    << op_gc_ms[OP_GB] << "," << op_gc_ms[OP_RB_BEST] << "," << op_gc_ms[OP_RB_WORST] << "," << op_gc_ms[OP_DIR] << ","
    << kb_to_mb(rss_peak) << "," << kb_to_mb(hwm_peak) << "\n";
}

static int run_server_real(const ServerConfig& cfg){
  std::cerr << ((cfg.party==emp::ALICE) ? "[S0] " : "[S1] ")
            << "listening client port="<<cfg.port_client
            <<", peer_port="<<cfg.port_peer
            <<", is_dir="<<cfg.is_dir
            <<"\n";

  const size_t rows = cfg.is_dir ? cfg.N : cfg.M;
  const size_t recB = cfg.is_dir ? cfg.D : cfg.B;
  if(!is_pow2(rows)) die("server: rows must be power of two");
  if(!cfg.is_dir && cfg.B < STATE_RECORD_STRUCT_BYTES) die("server: B too small for structured state record");

  std::vector<uint8_t> DB(rows * recB);
  for(size_t idx=0; idx<rows; idx++){
    std::vector<uint8_t> row = cfg.is_dir
      ? make_dir_record_bytes(idx, recB)
      : make_state_record_bytes(idx, cfg.M, cfg.N, recB);
    std::memcpy(DB.data() + idx*recB, row.data(), recB);
  }

  int lfd = listen_on(cfg.port_client);
  while(true){
    sockaddr_in cli{}; socklen_t clen=sizeof(cli);
    int cfd = accept(lfd, (sockaddr*)&cli, &clen);
    if(cfd<0) continue;

    uint32_t hdr[2]; recv_all(cfd, (uint8_t*)hdr, sizeof(hdr));
    uint32_t Q = hdr[0], ksz = hdr[1];

    std::vector<uint8_t> keybuf(ksz);
    std::array<uint8_t, QUERY_PAYLOAD_BYTES> payload_buf{};
    std::vector<uint8_t> reply(recB, 0);

    long rss_peak=-1, hwm_peak=-1;
    read_proc_status_kb(rss_peak, hwm_peak);

    double gc_ms_total = 0.0;
    uint64_t gc_bytes_total = 0;
    std::array<uint64_t, 5> op_counts{};
    std::array<uint64_t, 5> op_gc_bytes{};
    std::array<double, 5> op_gc_ms{};
    uint64_t allow_count = 0;
    uint64_t checksum_xor = 0;

    for(uint32_t qi=0; qi<Q; qi++){
      uint8_t op = 0;
      recv_all(cfd, &op, 1);
      recv_all(cfd, payload_buf.data(), payload_buf.size());
      recv_all(cfd, keybuf.data(), keybuf.size());

      DPFKey k = DPF_Deserialize(keybuf);
      std::fill(reply.begin(), reply.end(), 0);

      for(size_t idx=0; idx<rows; idx++){
        uint8_t sel = DPF_Eval(k, (uint64_t)idx, rows);
        if(sel){
          const uint8_t* rec = DB.data() + idx*recB;
          for(size_t j=0;j<recB;j++) reply[j] ^= rec[j];
        }
      }

      if(op < op_counts.size()) op_counts[op]++;

      if(!cfg.is_dir){
        PlainStateRecord local_share = decode_state_record_prefix(reply.data(), reply.size());
        QueryPayload local_payload = decode_query_payload(payload_buf);

        NetIO* gc_io = nullptr;
        if(cfg.party==emp::ALICE) gc_io = new NetIO(nullptr, cfg.port_peer);
        else                     gc_io = new NetIO(cfg.peer_host.c_str(), cfg.port_peer);

        uint64_t b0 = io_bytes(gc_io);
        auto t0 = std::chrono::steady_clock::now();
        CircuitPublicResult pub = run_state_circuit_real(cfg.party, gc_io, op, local_share, local_payload);
        auto t1 = std::chrono::steady_clock::now();
        uint64_t b1 = io_bytes(gc_io);

        double ms = std::chrono::duration<double, std::milli>(t1-t0).count();
        uint64_t by = (b1 - b0);

        gc_ms_total += ms;
        gc_bytes_total += by;
        if(op < op_gc_bytes.size()){
          op_gc_bytes[op] += by;
          op_gc_ms[op] += ms;
        }
        allow_count += pub.allow ? 1ULL : 0ULL;
        checksum_xor ^= pub.checksum;

        std::cerr << "[gc] op="<<optag_name(op)
                  << " allow="<<pub.allow
                  << " bucket="<<pub.selected_bucket
                  << " dir="<<pub.selected_dir_index
                  << " level="<<int(pub.next_level)
                  << " blockages="<<int(pub.next_blockages)
                  << " invites="<<int(pub.next_invites)
                  << " blocked="<<pub.next_blocked
                  << " gc_ms="<<ms
                  << " gc_bytes="<<by
                  << "\n";

        delete gc_io;
      }

      send_all(cfd, reply.data(), reply.size());

      long rss=-1,hwm=-1;
      if(read_proc_status_kb(rss,hwm)){
        rss_peak = std::max(rss_peak, rss);
        hwm_peak = std::max(hwm_peak, hwm);
      }
    }

    std::cerr << "[gc_total] gc_ms_total="<<gc_ms_total
          << " gc_bytes_total="<<gc_bytes_total
          << " allow_count="<<allow_count
          << " checksum_xor="<<checksum_xor
          << "\n";
    for(size_t op=1; op<op_counts.size(); op++){
      if(op_counts[op] == 0) continue;
      std::cerr << "[gc_total_op] op="<<optag_name((uint8_t)op)
                << " count="<<op_counts[op]
                << " gc_bytes="<<op_gc_bytes[op]
                << " gc_ms="<<op_gc_ms[op]
                << "\n";
    }
    std::cerr << "[server] peak VmRSS="<<kb_to_mb(rss_peak)<<" MB, peak VmHWM="<<kb_to_mb(hwm_peak)<<" MB\n";
    append_server_csv(cfg, Q, gc_ms_total, gc_bytes_total, op_counts, op_gc_bytes, op_gc_ms,
      allow_count, checksum_xor, rss_peak, hwm_peak);
    closesocket(cfd);
  }

  return 0;
}

// ============================================================
// Client
// ============================================================
static void csv_header(std::ofstream& f){
  f << "mode,M,B,N,D,lambda,iters,ops,bytes_sent,bytes_recv,total_time_ms,mean_iter_ms,rss_mb,hwm_mb\n";
}

struct ClientChan {
  int fd0=-1, fd1=-1;
  size_t rows=0, recB=0;
};

static ClientChan open_client_chan(const std::string& s0, const std::string& s1, size_t rows, size_t recB){
  auto [h0,p0] = parse_hostport(s0);
  auto [h1,p1] = parse_hostport(s1);
  ClientChan c;
  c.fd0 = dial(h0,p0);
  c.fd1 = dial(h1,p1);
  c.rows = rows;
  c.recB = recB;
  return c;
}
static void send_hdr(ClientChan& c, uint32_t Q, uint32_t ksz){
  uint32_t hdr[2] = { Q, ksz };
  send_all(c.fd0, (uint8_t*)hdr, sizeof(hdr));
  send_all(c.fd1, (uint8_t*)hdr, sizeof(hdr));
}
static void close_client_chan(ClientChan& c){
  if(c.fd0!=-1) closesocket(c.fd0);
  if(c.fd1!=-1) closesocket(c.fd1);
  c.fd0=c.fd1=-1;
}

static std::vector<uint8_t> parse_ops(const std::string& s){
  // default: gb,rb_best,rb_worst (and optional dir if enabled)
  std::vector<uint8_t> out;
  if(s.empty()) return out;
  size_t i=0;
  while(i<s.size()){
    size_t j = s.find(',', i);
    if(j==std::string::npos) j = s.size();
    std::string tok = s.substr(i, j-i);
    if(tok=="gb") out.push_back(OP_GB);
    else if(tok=="rb_best") out.push_back(OP_RB_BEST);
    else if(tok=="rb_worst") out.push_back(OP_RB_WORST);
    else if(tok=="dir") out.push_back(OP_DIR);
    else die("bad --ops token: "+tok+" (use gb,rb_best,rb_worst,dir)");
    i = j+1;
  }
  return out;
}

static std::string ops_to_string(const std::vector<uint8_t>& ops){
  std::string out;
  for(size_t i=0;i<ops.size();i++){
    if(i) out += ",";
    switch(ops[i]){
      case OP_GB: out += "gb"; break;
      case OP_RB_BEST: out += "rb_best"; break;
      case OP_RB_WORST: out += "rb_worst"; break;
      case OP_DIR: out += "dir"; break;
      default: out += "unk"; break;
    }
  }
  return out;
}

static uint64_t random_index(size_t rows){
  uint64_t idx = 0;
  PRG prg;
  prg.random_data(&idx, sizeof(idx));
  return idx & (rows - 1);
}

static QueryPayload make_redeem_payload(size_t state_idx, size_t M, size_t N, uint32_t client_tag){
  PlainStateRecord r = make_plain_state_record(state_idx, M, N);
  QueryPayload p;
  p.token_nonce = r.token_nonce;
  p.requested_dir_index = r.dir_index;
  p.token_epoch = r.token_epoch;
  p.flags = 1;
  p.client_tag = client_tag;
  return p;
}

static int run_iter_client(
  size_t iters,
  size_t M, size_t B,
  bool do_dirpir, size_t N, size_t D,
  int lambda_bits,
  const std::string& st0, const std::string& st1,
  const std::string& dir0, const std::string& dir1,
  const std::string& ops_str,
  const std::string& csv
){
  if(!is_pow2(M)) die("client: M must be power of two");
  if(do_dirpir && !is_pow2(N)) die("client: N must be power of two");

  std::vector<uint8_t> ops = parse_ops(ops_str);
  if(ops.empty()){
    ops = { OP_GB, OP_RB_BEST, OP_RB_WORST };
    if(do_dirpir) ops.push_back(OP_DIR);
  }
  std::string effective_ops = ops_to_string(ops);

  ClientChan st = open_client_chan(st0, st1, M, B);
  ClientChan dir;
  if(do_dirpir) dir = open_client_chan(dir0, dir1, N, D);

  // Determine key sizes
  auto [k0s,k1s] = DPF_Gen(0, M);
  auto b0s = DPF_Serialize(k0s);
  uint32_t ksz_st = (uint32_t)b0s.size();

  uint32_t ksz_dir = 0;
  if(do_dirpir){
    auto [k0d,k1d] = DPF_Gen(0, N);
    auto b0d = DPF_Serialize(k0d);
    ksz_dir = (uint32_t)b0d.size();
  }

  // Count how many state/dir queries per iter
  uint32_t per_iter_state = 0, per_iter_dir = 0;
  for(auto op: ops){
    if(op==OP_DIR){
      per_iter_state++;
      per_iter_dir++;
    } else {
      per_iter_state++;
    }
  }

  send_hdr(st, (uint32_t)(iters * per_iter_state), ksz_st);
  if(do_dirpir) send_hdr(dir, (uint32_t)(iters * per_iter_dir), ksz_dir);

  std::vector<uint8_t> r0(B), r1(B), out(B);
  std::vector<uint8_t> dr0(D), dr1(D), dout(D);

  auto send_tagged = [&](int fd, uint8_t tag, const std::array<uint8_t, QUERY_PAYLOAD_BYTES>& payload,
                         const std::vector<uint8_t>& kb){
    send_all(fd, &tag, 1);
    send_all(fd, payload.data(), payload.size());
    send_all(fd, kb.data(), kb.size());
  };

  using clk=std::chrono::steady_clock;
  auto t0 = clk::now();

  size_t bytes_sent=0, bytes_recv=0;
  double sum_iter_ms=0.0;

  auto one_pir = [&](ClientChan& c, uint64_t idx, size_t rows, size_t recB,
                     std::vector<uint8_t>& a0, std::vector<uint8_t>& a1, std::vector<uint8_t>& outbuf,
                     uint32_t expected_ksz,
                     uint8_t optag,
                     const QueryPayload& payload){
    auto [k0,k1] = DPF_Gen(idx, rows);
    auto b0 = DPF_Serialize(k0);
    auto b1 = DPF_Serialize(k1);
    if(b0.size()!=expected_ksz || b1.size()!=expected_ksz) die("DPF key size mismatch");

    auto [p0, p1] = split_query_payload(payload);
    send_tagged(c.fd0, optag, p0, b0);
    send_tagged(c.fd1, optag, p1, b1);
    bytes_sent += (1 + QUERY_PAYLOAD_BYTES + b0.size()) + (1 + QUERY_PAYLOAD_BYTES + b1.size());

    recv_all(c.fd0, a0.data(), recB); bytes_recv += recB;
    recv_all(c.fd1, a1.data(), recB); bytes_recv += recB;

    for(size_t j=0;j<recB;j++) outbuf[j] = (uint8_t)(a0[j] ^ a1[j]);
  };

  for(size_t it=0; it<iters; it++){
    auto tA = clk::now();

    for(size_t op_idx=0; op_idx<ops.size(); op_idx++){
      uint8_t op = ops[op_idx];
      uint64_t state_idx = random_index(M);
      QueryPayload zero_payload{};
      zero_payload.client_tag = (uint32_t)splitmix64(((uint64_t)it << 32) ^ ((uint64_t)op_idx << 8) ^ op);

      if(op==OP_DIR){
        if(!do_dirpir) die("client: --ops includes dir but --dirpir not enabled");
        QueryPayload redeem_payload = make_redeem_payload(
          state_idx, M, N,
          (uint32_t)splitmix64(((uint64_t)it << 32) ^ ((uint64_t)op_idx << 16) ^ 0x444952ULL));
        one_pir(st, state_idx, M, B, r0, r1, out, ksz_st, OP_DIR, redeem_payload);
        one_pir(dir, redeem_payload.requested_dir_index, N, D, dr0, dr1, dout, ksz_dir, OP_DIR, zero_payload);
      } else {
        one_pir(st, state_idx, M, B, r0, r1, out, ksz_st, op, zero_payload);
      }
    }

    auto tB = clk::now();
    double ms_iter = std::chrono::duration<double, std::milli>(tB-tA).count();
    sum_iter_ms += ms_iter;
  }

  auto t1 = clk::now();
  double ms_total = std::chrono::duration<double, std::milli>(t1-t0).count();
  double mean_iter_ms = sum_iter_ms/(double)iters;

  long rss_kb=-1,hwm_kb=-1;
  read_proc_status_kb(rss_kb,hwm_kb);

  std::cout
    << "iters="<<iters<<" M="<<M<<" B="<<B<<" lambda="<<lambda_bits
    << " dirpir="<<(do_dirpir?1:0)<<" N="<<N<<" D="<<D<<"\n"
    << "ops="<<effective_ops<<"\n"
    << "dpf_key_bytes_state="<<ksz_st<<" dpf_key_bytes_dir="<<ksz_dir<<"\n"
    << "bytes_sent="<<bytes_sent<<" bytes_recv="<<bytes_recv
    << " total_time_ms="<<ms_total
    << " mean_iter_ms="<<mean_iter_ms<<"\n"
    << "client_rss_mb="<<kb_to_mb(rss_kb)<<" client_hwm_mb="<<kb_to_mb(hwm_kb)<<"\n";

  if(!csv.empty()){
    std::ofstream f(csv, std::ios::app);
    if(f.tellp()==0) csv_header(f);
    f << "iter,"<<M<<","<<B<<","<<(do_dirpir?N:0)<<","<<(do_dirpir?D:0)<<","<<lambda_bits<<","<<iters<<",\""<<effective_ops<<"\","
      << bytes_sent << "," << bytes_recv << ","
      << ms_total << "," << mean_iter_ms << ","
      << kb_to_mb(rss_kb) << "," << kb_to_mb(hwm_kb)
      << "\n";
  }

  close_client_chan(st);
  if(do_dirpir) close_client_chan(dir);
  return 0;
}

int main(int argc, char** argv){
  std::string mode;
  int lambda_bits=128;

  ServerConfig scfg;

  // client
  size_t iters=100;
  size_t M=262144, B=128;
  bool dirpir=false;
  size_t N=65536, D=256;
  std::string st0="127.0.0.1:9001", st1="127.0.0.1:9002";
  std::string dir0="127.0.0.1:9101", dir1="127.0.0.1:9102";
  std::string ops_str; // e.g., "gb,rb_best"
  std::string csv;

  for(int i=1;i<argc;i++){
    std::string a(argv[i]);
    auto eatS=[&](const char* p, std::string& out){ std::string P(p); if(a.rfind(P,0)==0){ out=a.substr(P.size()); return true;} return false;};
    auto eatZ=[&](const char* p, size_t& out){ std::string P(p); if(a.rfind(P,0)==0){ out=std::stoull(a.substr(P.size())); return true;} return false;};
    auto eatI=[&](const char* p, int& out){ std::string P(p); if(a.rfind(P,0)==0){ out=std::stoi(a.substr(P.size())); return true;} return false;};
    if(a=="server"){ mode="server"; continue; }
    if(a=="client"){ mode="client"; continue; }

    if(eatI("--lambda=", lambda_bits)) continue;

    // server args
    if(eatI("--party=", scfg.party)) continue;
    if(eatI("--port_client=", scfg.port_client)) continue;
    if(eatS("--peer_host=", scfg.peer_host)) continue;
    if(eatI("--port_peer=", scfg.port_peer)) continue;
    if(a=="--dir_server"){ scfg.is_dir=true; continue; }
    if(eatZ("--M=", scfg.M)) continue;
    if(eatZ("--B=", scfg.B)) continue;
    if(eatZ("--N=", scfg.N)) continue;
    if(eatZ("--D=", scfg.D)) continue;
    if(eatS("--server_csv=", scfg.server_csv)) continue;

    // client args
    if(eatZ("--iters=", iters)) continue;
    if(eatZ("--cM=", M)) continue;
    if(eatZ("--cB=", B)) continue;
    if(a=="--dirpir"){ dirpir=true; continue; }
    if(eatZ("--cN=", N)) continue;
    if(eatZ("--cD=", D)) continue;
    if(eatS("--st0=", st0)) continue;
    if(eatS("--st1=", st1)) continue;
    if(eatS("--dir0=", dir0)) continue;
    if(eatS("--dir1=", dir1)) continue;
    if(eatS("--ops=", ops_str)) continue;
    if(eatS("--csv=", csv)) continue;
  }

  if(mode=="server"){
    if(scfg.party!=emp::ALICE && scfg.party!=emp::BOB) die("--party must be 1 (ALICE) or 2 (BOB)");
    const size_t rows = scfg.is_dir ? scfg.N : scfg.M;
    if(!is_pow2(rows)) die("server: DB size must be power of two");
    return run_server_real(scfg);
  }

  if(mode=="client"){
    return run_iter_client(iters, M, B, dirpir, N, D, lambda_bits, st0, st1, dir0, dir1, ops_str, csv);
  }

  std::cerr <<
    "usage:\n"
    "  server --party=1|2 --port_client=PORT --peer_host=IP --port_peer=PORT [--dir_server]\n"
    "         --M=262144 --B=128 (state) or --N=65536 --D=256 (dir) [--server_csv=PATH]\n"
    "  client --iters=1 --cM=262144 --cB=128 --st0=IP:PORT --st1=IP:PORT\n"
    "         [--dirpir --cN=65536 --cD=256 --dir0=IP:PORT --dir1=IP:PORT]\n"
    "         [--ops=gb,rb_best,rb_worst,dir]\n";
  return 2;
}
