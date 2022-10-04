use std::net::SocketAddr;

pub(crate) fn get_random_port() -> u16 {
    let socket = socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::STREAM, None).unwrap();
    socket.set_nonblocking(true).unwrap();

    /* See https://stackoverflow.com/a/14388707/6094756.
     * On most BSD and Linux systems, we need both REUSEADDR and REUSEPORT;
     * and if they don't support the latter we won't compile.
     * On Windows, there is only REUSEADDR but it does what we want.
     */
    socket.set_reuse_address(true).unwrap();
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    {
        socket.set_reuse_port(true).unwrap();
    }
    #[cfg(not(any(
        all(unix, not(any(target_os = "solaris", target_os = "illumos"))),
        target_os = "windows"
    )))]
    {
        compile_error!("Your system is not supported yet, please raise an error");
    }

    socket
        .bind(&"[::]:0".parse::<SocketAddr>().unwrap().into())
        .unwrap();
    socket.local_addr().unwrap().as_socket().unwrap().port()
}
