;; source files
(set @m_files     (filelist "^objc/.*.m$"))
(set @c_files     (filelist "^objc/.*.c$"))
(set @nu_files 	  (filelist "^nu/.*nu$"))

(set SYSTEM ((NSString stringWithShellCommand:"uname") chomp))
(case SYSTEM
      ("Darwin"
               (set @arch (list "x86_64"))
               (set @cflags "-I ./src -g -std=gnu99 -fobjc-arc -DDARWIN")
               (set @ldflags  "-framework Foundation -framework Nu"))
      ("Linux"
              (set @arch (list "i386"))
              (set gnustep_flags ((NSString stringWithShellCommand:"gnustep-config --objc-flags") chomp))
              (set gnustep_libs ((NSString stringWithShellCommand:"gnustep-config --base-libs") chomp))
              (set @cflags "-I ./src -g -std=gnu99 -DLINUX -I/usr/local/include #{gnustep_flags}")
              (set @ldflags "#{gnustep_libs} -lNu"))
      (else nil))

;; framework description
(set @framework "RadMongoDB")
(set @framework_identifier "com.radtastical.radmongodb")
(set @framework_creator_code "????")
(set @public_headers (filelist "^headers/.*\.h$"))

(compilation-tasks)
(framework-tasks)

(task "clobber" => "clean" is
      (SH "rm -rf #{@framework_dir}"))

(task "default" => "framework")

(task "doc" is (SH "nudoc"))

