
get.adcon.data_synop <- function(conn, dirAWS, dirUP = NULL, upload = TRUE){
    on.exit({
        if(upload) ssh::ssh_disconnect(session)
    })
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "adcon_db_aws_synop_list.csv")
    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "adcon_db_synop_parameters_table.csv")
    # dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA0", "ADCON_SYNOP")
    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "ADCON_SYNOP")

    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON_SYNOP")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_DATA", "AUTH", "adt.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
        if(inherits(session, "try-error")){
            logUpload <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON_SYNOP", "processing_adcon_synop.txt")
            msg <- paste(session, "Unable to connect to ADT server\n")
            format.out.msg(msg, logUpload)

            upload <- FALSE
        }

        dirADT <- file.path(dirUP, "AWS_DATA", "DATA", "ADCON_SYNOP")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirADT, ' ] ; then mkdir -p ', dirADT, ' ; fi')
        ))
    }

    awsInfo <- utils::read.table(awsFile, header = TRUE, sep = ",", na.strings = "",
                                 stringsAsFactors = FALSE, quote = "\"")
    varTable <- utils::read.table(varFile, header = TRUE, sep = ",", na.strings = "",
                                  stringsAsFactors = FALSE, quote = "\"")

    lastDate <- as.integer(awsInfo$last)
    awsID <- awsInfo$id

    for(j in seq_along(awsID)){
        varAWS <- varTable[varTable$id == awsID[j], , drop = FALSE]

        qres <- lapply(seq_along(varAWS$node_var), function(i){
            if(is.na(lastDate[j])){
                query <- paste0("SELECT * FROM historiandata WHERE tag_id=", varAWS$node_var[i])
            }else{
                query <- paste0("SELECT * FROM historiandata WHERE tag_id=", varAWS$node_var[i],
                                " AND enddate > ", lastDate[j])
            }

            qres <- try(DBI::dbGetQuery(conn, query), silent = TRUE)
            if(inherits(qres, "try-error")){
                msg <- paste("Unable to get data for", awsID[j], "parameter", varAWS$var_name[i])
                format.out.msg(msg, awsLOG)
                return(NULL)
            }

            if(nrow(qres) == 0) return(NULL)

            valid <- qres$tag_id == varAWS$node_var[i] & qres$status == 0
            qres <- qres[valid, , drop = FALSE]

            if(nrow(qres) == 0) return(NULL)

            end <- as.POSIXct(qres$enddate, origin = origin, tz = tz)
            start <- as.POSIXct(qres$startdate, origin = origin, tz = tz)
            dft <- difftime(end, start, units = "mins")
            ## 15 min interval, take obs with greater than 10 mins sampling and less than 20
            qres <- qres[dft > 10 & dft < 20, , drop = FALSE]

            if(nrow(qres) == 0) return(NULL)

            qres <- qres[, c('enddate', 'measuringvalue'), drop = FALSE]

            ## round to 15 minutes (0, 15, 30, 45)
            # end <- as.POSIXct(qres$enddate, origin = origin, tz = tz)
            # mins <- as.numeric(format(end, '%M'))

            return(qres)
        })

        inull <- sapply(qres, is.null)
        if(all(inull)) next

        qres <- qres[!inull]
        varAWS <- varAWS[!inull, , drop = FALSE]

        nvar <- c('id', "var_height", "var_code", "stat_code", "min_val", "max_val")
        qres <- lapply(seq_along(varAWS$node_var), function(i){
            x <- qres[[i]]
            v <- varAWS[i, nvar, drop = FALSE]
            v <- v[rep(1, nrow(x)), , drop = FALSE]
            cbind(v, x)
        })

        qres <- do.call(rbind, qres)
        ## adcon synop network
        qres$network <- 1

        out <- parse.adcon_db.data(qres)

        awsInfo$last[j] <- max(out$obs_time)

        locFile <- paste0(awsID[j], "_", paste(range(out$obs_time), collapse = "_"))
        # locFile <- paste0(awsID[j], ".rds")
        locFile <- file.path(dirOUT, locFile)
        saveRDS(out, locFile)

        if(upload){
            adtFile <- file.path(dirADT, basename(locFile))
            ssh::scp_upload(session, locFile, to = adtFile, verbose = FALSE)
        }
    }

    utils::write.table(awsInfo, awsFile, sep = ",", na = "", col.names = TRUE,
                       row.names = FALSE, quote = FALSE)
    if(upload){
        adtFile <- file.path(dirUP, "AWS_DATA", "CSV", basename(awsFile))
        ssh::scp_upload(session, awsFile, to = adtFile, verbose = FALSE)
    }

    return(0)
}
