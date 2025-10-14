local _M = {}

function _M.table_contains(t, value)
    for _, v in pairs(t) do
        if v == value then
            return true
        end
    end
    return false
end

function _M.coerce_positive_integer(value, default)
    local num = tonumber(value)
    return (num and num > 0 and num == math.floor(num) and num ~= math.huge) and num or (default or 0)
end

return _M