get.tahmo.data <- function(dirFTP, dirAWS, dirUP = NULL, upload = TRUE){
    on.exit({
        if(upload) ssh::ssh_disconnect(session)
    })
    tz <- Sys.getenv("TZ")
    origin <- "1970-01-01"

    awsFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_aws_list.csv")
    varFile <- file.path(dirAWS, "AWS_DATA", "CSV", "tahmo_parameters_table.csv")
    dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA", "TAHMO")
    # dirOUT <- file.path(dirAWS, "AWS_DATA", "DATA0", "TAHMO")
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

    lastDate <- as.integer(awsInfo$last)
    startDate <- as.Date(as.POSIXct(lastDate, origin = origin, tz = tz))
    startDate[is.na(startDate)] <- as.Date("2015-01-01")
    endDate <- as.Date(Sys.time())

    # awsID <- awsInfo$id
    # awsDIR <- paste0(awsInfo$dirName, "-", awsInfo$nodeID)
    # awsFILES <- awsInfo$dirName

    varTable <- var.network.table(varFile)

    ##########

    ##########

    return(0)
}
