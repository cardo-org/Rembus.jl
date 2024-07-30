#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

const VERSION = "0.4.0"

#const Rembus.CONFIG = Rembus.Settings()

const DATAFRAME_TAG = 80

const REMBUS_CA = "rembus-ca.crt"

const PING_INTERVAL = 10
const PONG_STRING = "*_pong_*"

const REMBUS_PAGE_SIZE = 1_000_000

const MESSAGE_CHANNEL_SZ = 1000

const BROKER_CONFIG = "__config__"
const CID = "cid"
const COMMAND = "cmd"
const DATA = :data
const RETROACTIVE = "retroactive"
const STATUS = "status"

const REACTIVE_CMD = "reactive"
const ENABLE_ACK_CMD = "enable_ack"
const DISABLE_ACK_CMD = "disable_ack"
const RESET_ROUTER_CMD = "reset_router"
const SHUTDOWN_CMD = "shutdown"
const ENABLE_DEBUG_CMD = "enable_debug"
const DISABLE_DEBUG_CMD = "disable_debug"
const UPTIME_CMD = "uptime"
const SUBSCRIBE_CMD = "subscribe"
const UNSUBSCRIBE_CMD = "unsubscribe"
const EXPOSE_CMD = "expose"
const UNEXPOSE_CMD = "unexpose"
const AUTHORIZE_CMD = "authorize"
const UNAUTHORIZE_CMD = "unauthorize"
const PRIVATE_TOPIC_CMD = "private_topic"
const PUBLIC_TOPIC_CMD = "public_topic"
const PRIVATE_TOPICS_CONFIG_CMD = "private_topics_config"
const BROKER_CONFIG_CMD = "broker_config"
const LOAD_CONFIG_CMD = "load_config"
const SAVE_CONFIG_CMD = "save_config"

const REACTIVE_HANDLER = "reactive_handler"
const SUBSCRIBE_HANDLER = "subscribe_handler"
const UNSUBSCRIBE_HANDLER = "unsubscribe_handler"
const EXPOSE_HANDLER = "expose_handler"
const UNEXPOSE_HANDLER = "unexpose_handler"
const AUTHORIZE_HANDLER = "authorize_handler"
const UNAUTHORIZE_HANDLER = "unauthorize_handler"
const PRIVATE_TOPIC_HANDLER = "private_topic_handler"
const PUBLIC_TOPIC_HANDLER = "public_topic_handler"

const TYPE_IDENTITY::UInt8 = 0
const TYPE_PUB::UInt8 = 1
const TYPE_RPC::UInt8 = 2
const TYPE_ADMIN::UInt8 = 3
const TYPE_RESPONSE::UInt8 = 4
const TYPE_ACK::UInt8 = 5
const TYPE_UNREGISTER::UInt8 = 9
const TYPE_REGISTER::UInt8 = 10
const TYPE_ATTESTATION::UInt8 = 11

# ZeroMQ periodic ping
const TYPE_PING::UInt8 = 12
const TYPE_PONG::UInt8 = 13

const TYPE_REMOVE::UInt8 = 14
const TYPE_CLOSE::UInt8 = 15

const REACTIVE_DISABLE::Bool = false
const REACTIVE_ENABLE::Bool = true

const ACK_WAIT_TIME = 0.5

const STS_SUCCESS::UInt8 = 0
const STS_GENERIC_ERROR::UInt8 = 10
const STS_CHALLENGE::UInt8 = 11
const STS_IDENTIFICATION_ERROR::UInt8 = 20
const STS_METHOD_EXCEPTION::UInt8 = 40
const STS_METHOD_NOT_FOUND::UInt8 = 42
const STS_METHOD_UNAVAILABLE::UInt8 = 43
const STS_METHOD_LOOPBACK::UInt8 = 44
const STS_TARGET_NOT_FOUND::UInt8 = 45
const STS_TARGET_DOWN::UInt8 = 46
const STS_UNKNOWN_ADMIN_CMD::UInt8 = 47
const STS_NAME_ALREADY_TAKEN::UInt8 = 60

# Rembus timeout
const STS_TIMEOUT::UInt8 = 70

const ACK_FLAG = 0x10

const TYPE_0 = zero(UInt8)
const TYPE_1 = one(UInt8) << 5
const TYPE_2 = UInt8(2) << 5
const TYPE_3 = UInt8(3) << 5
const TYPE_4 = UInt8(4) << 5
const TYPE_5 = UInt8(5) << 5
const TYPE_6 = UInt8(6) << 5
const TYPE_7 = UInt8(7) << 5

const BITS_PER_BYTE = UInt8(8)
const HEX_BASE = Int(16)
const LOWEST_ORDER_BYTE_MASK = 0xFF

const TYPE_BITS_MASK = UInt8(0b1110_0000)
const ADDNTL_INFO_MASK = UInt8(0b0001_1111)

const ADDNTL_INFO_UINT8 = UInt8(24)
const ADDNTL_INFO_UINT16 = UInt8(25)
const ADDNTL_INFO_UINT32 = UInt8(26)
const ADDNTL_INFO_UINT64 = UInt8(27)

const SINGLE_BYTE_SIMPLE_PLUS_ONE = UInt8(24)
const SIMPLE_FALSE = UInt8(20)
const SIMPLE_TRUE = UInt8(21)
const SIMPLE_NULL = UInt8(22)
const SIMPLE_UNDEF = UInt8(23)

const ADDNTL_INFO_FLOAT16 = UInt8(25)
const ADDNTL_INFO_FLOAT32 = UInt8(26)
const ADDNTL_INFO_FLOAT64 = UInt8(27)

const ADDNTL_INFO_INDEF = UInt8(31)
const BREAK_INDEF = TYPE_7 | UInt8(31)

const SINGLE_BYTE_UINT_PLUS_ONE = 24
const UINT8_MAX_PLUS_ONE = 0x100
const UINT16_MAX_PLUS_ONE = 0x10000
const UINT32_MAX_PLUS_ONE = 0x100000000
const UINT64_MAX_PLUS_ONE = 0x10000000000000000

const INT8_MAX_POSITIVE = 0x7f
const INT16_MAX_POSITIVE = 0x7fff
const INT32_MAX_POSITIVE = 0x7fffffff
const INT64_MAX_POSITIVE = 0x7fffffffffffffff

const SIZE_OF_FLOAT64 = sizeof(Float64)
const SIZE_OF_FLOAT32 = sizeof(Float32)
const SIZE_OF_FLOAT16 = sizeof(Float16)

const POS_BIG_INT_TAG = UInt8(2)
const NEG_BIG_INT_TAG = UInt8(3)

const CBOR_FALSE_BYTE = UInt8(TYPE_7 | 20)
const CBOR_TRUE_BYTE = UInt8(TYPE_7 | 21)
const CBOR_NULL_BYTE = UInt8(TYPE_7 | 22)
const CBOR_UNDEF_BYTE = UInt8(TYPE_7 | 23)


const CUSTOM_LANGUAGE_TYPE = 27
