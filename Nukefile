;; source files
(set @m_files     (filelist "^objc/.*.m$"))
(set @c_files     (filelist "^objc/.*.c$"))
(set @nu_files 	  (filelist "^nu/.*nu$"))

(set SYSTEM ((NSString stringWithShellCommand:"uname") chomp))
(case SYSTEM
      ("Darwin"
               (set @arch (list "x86_64"))
               (set @cflags "-DDARWIN -I ./objc -g -fobjc-arc")
               (set @ldflags  "-framework Foundation -framework Nu"))
      ("Linux"
              (set @arch (list "i386"))
              (set gnustep_flags ((NSString stringWithShellCommand:"gnustep-config --objc-flags") chomp))
              (set gnustep_libs ((NSString stringWithShellCommand:"gnustep-config --base-libs") chomp))
              (set @cflags "-DLINUX -I . -I ./objc -g -fobjc-arc -fobjc-nonfragile-abi -fblocks #{gnustep_flags}")
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

