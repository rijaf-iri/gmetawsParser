
parse.adcon_db.data <- function(tmp){
    tmp$min_val[is.na(tmp$min_val)] <- -Inf
    tmp$max_val[is.na(tmp$max_val)] <- Inf

    tmp$limit_check <- NA
    tmp$raw_value <- tmp$measuringvalue

    ## limit check
    tmp$limit_check[tmp$measuringvalue < tmp$min_val] <- 1
    tmp$limit_check[tmp$measuringvalue > tmp$max_val] <- 2

    tmp <- tmp[, c("network", "id", "var_height", "enddate", "var_code",
                   "stat_code", "measuringvalue", "raw_value", "limit_check")]

    fun_format <- list(as.integer, as.character, as.numeric, as.integer,
                       as.integer, as.integer, as.numeric, as.numeric, as.integer)

    tmp <- lapply(seq_along(fun_format), function(j) fun_format[[j]](tmp[[j]]))
    tmp <- as.data.frame(tmp)

    names(tmp) <- c("network", "id", "height", "obs_time", "var_code",
                    "stat_code", "value", "raw_value", "limit_check")

    tmp$obs_id <- getObsId(tmp)

    tmp <- tmp[!duplicated(tmp$obs_id), , drop = FALSE]
    tmp <- tmp[order(tmp$obs_time), , drop = FALSE]

    return(tmp)
}
