package consts

enum class ErrorCode(val code: Int) {
    /**
     * Messages errors
     */
    CREATE_MESSAGE(0),
    GET_MESSAGE(1),
    GET_ALL_MESSAGE(2),
    UPDATE_MESSAGE(3),
    DELETE_MESSAGE(4),
    NO_SUCH_MESSAGE(5)
}
