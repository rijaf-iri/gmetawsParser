#' Process ADCON data.
#'
#' Get the data from ADCON database, parse and insert into ADT database.
#' 
#' @param dirFTP full path to the directory containing the Adcon data.\cr
#' @param dirAWS full path to the directory containing the AWS_DATA folder on Adcon server.\cr
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.\cr
#' @param upload logical, if TRUE the data will be uploaded to ADT server.
#' 
#' @export

process.adcon <- function(dirFTP, dirAWS, dirUP = NULL, upload = TRUE){
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "ADCON")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_adcon.txt")

    ret <- try(get.adcon.data(dirFTP, dirAWS, dirUP, upload), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- paste(ret, "Getting ADCON data failed")
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}

#' Process TAHMO data.
#'
#' Get the data from TAHMO database, parse and insert into ADT database.
#' 
#' @param dirFTP full path to the directory containing the Tahmo data.\cr
#' @param dirAWS full path to the directory containing the AWS_DATA folder on Tahmo server.\cr
#' @param dirUP full path to the directory containing the AWS_DATA folder on ADT server.
#'              Default NULL, must be provided if \code{upload} is \code{TRUE}.\cr
#' @param upload logical, if TRUE the data will be uploaded to ADT server.
#' 
#' @export

process.tahmo <- function(dirFTP, dirAWS, dirUP = NULL, upload = TRUE){
    dirLOG <- file.path(dirAWS, "AWS_DATA", "LOG", "TAHMO")
    if(!dir.exists(dirLOG))
        dir.create(dirLOG, showWarnings = FALSE, recursive = TRUE)
    logPROC <- file.path(dirLOG, "processing_tahmo.txt")

    ret <- try(get.tahmo.data(dirFTP, dirAWS, dirUP, upload), silent = TRUE)
    if(inherits(ret, "try-error")){ 
        mserr <- gsub('[\r\n]', '', ret[1])
        msg <- paste(ret, "Getting TAHMO data failed")
        format.out.msg(paste(mserr, '\n', msg), logPROC)
        return(2)
    }

    return(0)
}
