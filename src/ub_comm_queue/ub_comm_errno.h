#ifndef UB_COMM_ERRNO_H
#define UB_COMM_ERRNO_H

#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Return code convention:
 *  0                  : success
 *  >0                 : success with hint
 *  -errno             : standard Linux errno-compatible error
 *  <= -4096           : ub_comm_queue private business error
 *
 * Recommended mapping table:
 *  -EINVAL            : invalid argument / invalid configuration
 *  -ENOMEM            : memory allocation failure / ring region out of memory
 *  -EMSGSIZE          : message length exceeds ring capacity
 *  -EOPNOTSUPP        : reserved or unsupported operation/message type
 *  -EPERM             : sender identity does not match current node
 *  -ENOENT            : callback not registered
 *  -EFAULT            : shared memory access failed / unknown internal fault
 *  -EEXIST            : duplicate ring priority
 *  -EIO               : remote ring metadata is corrupted
 *  -EPIPE             : async worker pool unavailable
 *
 *  UB_COMM_ERR_RING_FULL          : ring is full
 *  UB_COMM_ERR_RING_BUSY          : remote enqueue CAS retries exceeded
 *  UB_COMM_ERR_PEER_NODE_NOT_FOUND: destination node does not exist in node map
 *  UB_COMM_ERR_PEER_NOT_READY     : destination node exists but is not ready
 *  UB_COMM_ERR_RING_NOT_FOUND     : destination ring for the priority does not exist
 */

#define UB_COMM_OK 0
#define UB_COMM_SEND_CONGESTED 1

#define UB_COMM_ERR_PRIVATE_BASE 4096

#define UB_COMM_ERR_RING_FULL (-UB_COMM_ERR_PRIVATE_BASE)
#define UB_COMM_ERR_RING_BUSY (-(UB_COMM_ERR_PRIVATE_BASE + 1))
#define UB_COMM_ERR_PEER_NODE_NOT_FOUND (-(UB_COMM_ERR_PRIVATE_BASE + 2))
#define UB_COMM_ERR_PEER_NOT_READY (-(UB_COMM_ERR_PRIVATE_BASE + 3))
#define UB_COMM_ERR_RING_NOT_FOUND (-(UB_COMM_ERR_PRIVATE_BASE + 4))

#ifdef __cplusplus
}
#endif

#endif /* UB_COMM_ERRNO_H */
