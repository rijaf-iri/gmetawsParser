
get.tahmo.data <- function(dirAWS){
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"
    dataset <- 'controlled'

    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "TAHMO")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_aws_list.csv")
    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_parameters_table.csv")

    awsInfo <- utils::read.table(awsFile, sep = ',', header = TRUE,
                                 stringsAsFactors = FALSE,
                                 colClasses = "character", quote = "\"")
    varTable <- var.network.table(varFile)

    api <- tahmo.api(dirAWS)$connection
    now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ")

    for(j in seq_along(awsInfo$id)){
        awsID <- awsInfo$id[j]
        endpoint <- paste('services/measurements/v2/stations',
                           awsID, 'measurements', dataset, sep = '/')
        api_url <- paste0(api$url, "/", endpoint)

        last <- awsInfo$last[j]
        if(is.na(last)){
            last <- "2022-01-01T00:00:00Z"
        }else{
            last <- as.POSIXct(as.integer(last), origin = origin, tz = tz) + 60
            last <- format(last, "%Y-%m-%dT%H:%M:%SZ")
        }

        qres <- httr::GET(api_url, httr::accept_json(),
                httr::authenticate(api$id, api$secret),
                query = list(start = last, end = now))
        if(httr::status_code(qres) != 200) next

        resc <- httr::content(qres)
        if(is.null(resc$results[[1]]$series)) next
        # if(resc$results[[1]]$statement_id != 0) next

        hdr <- unlist(resc$results[[1]]$series[[1]]$columns)
        val <- lapply(resc$results[[1]]$series[[1]]$values, function(x){
            sapply(x, function(v) if(is.null(v)) NA else v)
        })
        val <- do.call(rbind, val)
        val <- as.data.frame(val)
        names(val) <- hdr
        ic <- c('time', 'value', 'variable', 'quality')

        out <- try(parse.tahmo.data(val[, ic], awsID, varTable), silent = TRUE)
        if(inherits(out, "try-error")){
            mserr <- gsub('[\r\n]', '', out[1])
            msg <- paste("Unable to parse data for", awsID)
            format.out.msg(paste(mserr, '\n', msg), awsLOG)
            return(NULL)
        }

        if(is.null(out)) next
        awsInfo$last[j] <- max(out$obs_time)

        locFile <- paste0(awsID, "_", paste(range(out$obs_time), collapse = "_"))
        locFile <- file.path(dirOUT, locFile)
        saveRDS(out, locFile)
    }

    utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                       row.names = FALSE, quote = FALSE)
    return(0)
}
