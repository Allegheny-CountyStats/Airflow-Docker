require(rmarkdown)

rmd <- Sys.getenv('rmd')

render(rmd)

system("python3 sharepoint-upload.py")

system("python3 send_email_RUN.py")