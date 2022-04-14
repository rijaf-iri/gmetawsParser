
get.tahmo.data0 <- function(dirTAHMO, dirAWS, dirUP = NULL, upload = TRUE){
    on.exit({
        if(upload) ssh::ssh_disconnect(session)
    })
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_aws_list.csv")
    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_parameters_table.csv")
    # dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "TAHMO")
    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA0", "TAHMO")
    if(!dir.exists(dirOUT))
        dir.create(dirOUT, showWarnings = FALSE, recursive = TRUE)
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    awsLOG <- file.path(dirLOG, "AWS_LOG.txt")

    if(upload){
        ssh <- readRDS(file.path(dirAWS, "AWS_DATA", "AUTH", "adt.cred"))
        session <- try(do.call(ssh::ssh_connect, ssh$cred), silent = TRUE)
        if(inherits(session, "try-error")){
            logUpload <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO", "processing_tahmo.txt")
            msg <- paste(session, "Unable to connect to ADT server\n")
            format.out.msg(msg, logUpload)

            upload <- FALSE
        }

        dirADT <- file.path(dirUP, "AWS_DATA", "DATA", "TAHMO")
        ssh::ssh_exec_wait(session, command = c(
            paste0('if [ ! -d ', dirADT, ' ] ; then mkdir -p ', dirADT, ' ; fi')
        ))
    }

    awsInfo <- utils::read.table(awsFile, header = TRUE, sep = ",", na.strings = "",
                                 stringsAsFactors = FALSE, quote = "\"")
    awsID <- awsInfo$id
    varTable <- var.network.table(varFile)

    for(j in seq_along(awsID)){
        aws_pth <- file.path(dirTAHMO, awsID[j])
        allfiles <- list.files(aws_pth, ".+\\.csv$")
        if(length(allfiles) == 0) next

        out <- lapply(seq_along(allfiles), function(i){
            file_csv <- file.path(aws_pth, allfiles[i])
            qres <- utils::read.table(file_csv, sep = ",", header = TRUE)

            if(nrow(qres) == 0) return(NULL)

            aws.dat <- try(parse.tahmo.data0(qres, awsID[j], varTable), silent = TRUE)

            if(inherits(aws.dat, "try-error")){
                mserr <- gsub('[\r\n]', '', aws.dat[1])
                msg <- paste("Unable to parse data for", awsID[j], ":", allfiles[i])
                format.out.msg(paste(mserr, '\n', msg), awsLOG)
                return(NULL)
            }

            aws.dat
        })

        out <- do.call(rbind, out)
        if(is.null(out)) next

        awsInfo$last[j] <- max(out$obs_time)

        # locFile <- paste0(awsID[j], "_", paste(range(out$obs_time), collapse = "_"))
        locFile <- paste0(awsID[j], ".rds")
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
