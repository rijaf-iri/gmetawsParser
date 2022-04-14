
parse.tahmo.data0 <- function(qres, awsID, varTable){
    tz <- Sys.getenv("TZ")

    temps <- strptime(qres$time, "%Y-%m-%dT%H:%M:%SZ", tz = tz)
    ina <- is.na(temps)
    qres <- qres[!ina, , drop = FALSE]

    if(nrow(qres) == 0) return(NULL)

    temps <- temps[!ina]
    ivar <- qres$variable %in% varTable$parameter_code
    tmp <- qres[ivar, , drop = FALSE]
    tmp$time <- as.POSIXct(temps[ivar])
    tmp <- tmp[!is.na(tmp$value), , drop = FALSE]

    if(nrow(tmp) == 0) return(NULL)

    ix <- match(tmp$variable, varTable$parameter_code)

    var_nm <- c("var_height", "var_code", "stat_code", "min_val", "max_val")
    var_dat <- varTable[ix, var_nm, drop = FALSE]
    tmp <- cbind(tmp, var_dat)

    ## convert soil moisture m^3/m^3 to %
    sm <- tmp$var_code == 7
    tmp$value[sm] <- tmp$value[sm] * 100

    ## convert relative humidity fraction to %
    rh <- tmp$var_code == 6
    tmp$value[rh] <- tmp$value[rh] * 100

    ## convert pressure kPa to hPa
    ap <- tmp$var_code == 1
    tmp$value[ap] <- tmp$value[ap] * 10

    ######

    tmp$limit_check <- NA
    tmp$network <- 3
    tmp$id <- awsID
    tmp$raw_value <- tmp$value

    ###### limit check

    tmp$min_val[is.na(tmp$min_val)] <- -Inf
    tmp$max_val[is.na(tmp$max_val)] <- Inf

    tmp$limit_check[tmp$value < tmp$min_val] <- 1
    tmp$limit_check[tmp$value > tmp$max_val] <- 2

    ######

    tmp <- tmp[, c("network", "id", "var_height", "time", "var_code",
                   "stat_code", "value", "raw_value", "limit_check")]

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
